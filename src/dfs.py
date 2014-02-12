#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
   Author: Richard J. marini (richardjmarini@gmail.com)
   Date: 4/1/2013
   Name: Dynamic Frequency Scaler
   Desc: Dynamically spins up/down instances (currencly ec2 aws instances)
   based on the number of currently waiting tasks to process
'''

from datetime import datetime
from boto.ec2 import EC2Connection
from time import sleep
from threading import Thread
from os import path, getcwd, pardir, makedirs, getpid
from sys import exit, argv, stderr, stdout, stdin
from multiprocessing.managers import SyncManager 
from multiprocessing import Value, JoinableQueue, Process, Lock
from optparse import OptionParser, make_option
from subprocess import Popen

from logger import log
from daemon import Daemon

class DFSManager(SyncManager):
   '''
   Implements "dynamic frequency scaling" via ec2 instances
   '''
   def __init__(self, opts):
      '''
      Initializes DFSManager
      '''

      self.opts= opts
      self.alive= True
      self.instances= dict()
      self.sleep= self.opts.sleep

      self.connect()

      # now register with dfs
      self.register('getInstances', callable= lambda: self.instances)
      self.dfsInstance= self.opts.dfsInstance
      (self.dfsHost, self.dfsPort, self.dfsKey)= self.opts.dfs.split(':')
      super(DFSManager, self).__init__(address= (self.dfsHost, int(self.dfsPort)), authkey= self.dfsKey)

      # connect to aws ec2
      (
         self.ec2Access,
         self.ec2Secret,
         self.ec2GroupName,
         self.ec2KeyName,
         self.ec2AMI,
         self.ec2InstanceType,
         self.ec2BootStrapFile
      )= self.opts.ec2.split(':')
      try:
         self.ec2Conn= EC2Connection(self.ec2Access, self.ec2Secret)
      except Exception, e:
         log("warn", "could not connect to ec2: %s" % (str(e)))


      # setup the pipeline monitor thread
      self.pipelineMonitor= Thread(target= self.pipelineMonitor)
      #self.pipelineMonitor= Process(target= self.pipelineMonitor)

   def connect(self):

      # register with Queue
      SyncManager.register('getPipeline')
      SyncManager.register('getStore')
      self.qInstance= self.opts.qInstance
      (self.qHost, self.qPort, self.qKey)= self.opts.queue.split(':')
      queue= SyncManager(address= (self.qHost, int(self.qPort)), authkey= self.qKey)
      queue.connect()
      self.pipeline= queue.getPipeline()
      self.store= queue.getStore()


   def getBootStrap(self, filename= None):
      '''
      Returns the bootStrap code for the ec2 instance
      '''
      bootStrap= ""

      if filename == None:
         log("warn", "no bootStrap file provided")
         return bootStrap
          
      if path.exists(filename):

         try:
            fh= open(filename, 'r')
            bootStrap= fh.read()
            fh.close()

            '''
            The node needs to reverse lookup the host name based
            on the 
            '''
            dfs= '%s:%s:%s' % (self.dfsHost, self.dfsPort, self.dfsKey)
            if self.dfsInstance != None:
               (reservation, )= self.ec2Conn.get_all_instances(instance_ids= [self.qInstance])
               (instance, )= reservation.instances
               dfs= '%s:%s:%s' % (instance.public_dns_name, self.dfsPort, self.dfsKey)

            queue= '%s:%s:%s' % (self.qHost, self.qPort, self.qKey)
            if self.qInstance != None:
               (reservation, )= self.ec2Conn.get_all_instances(instance_ids= [self.dfsInstance])
               (instance, )= reservation.instances
               queue= '%s:%s:%s' % (instance.public_dns_name, self.qPort, self.qKey)
             
            log("info", "BOOTSTRAP QUEUE: %s, DFS: %s" % (queue, dfs))
            bootStrap= bootStrap % dict(queue= queue, dfs= dfs, s3= self.opts.s3, maxProcesses= self.opts.maxProcesses, sleep= self.opts.sleep)
         except Exception, e:
            log("error", "could not parse bootStrap: %s" % (str(e)))
            pass

      else:

         log("warn", "bootStrap file %s not found" % (filename))
         pass

      return bootStrap

   def startInstances(self, numInstances= 1):
      '''
      Spins up 'N' instances of Node 
      '''

      log("trace", "begin")

      log("info", "starting %s instances" % (numInstances))

      ec2BootStrap= self.getBootStrap(self.ec2BootStrapFile)
      
      try:
         image= self.ec2Conn.get_image(self.ec2AMI)

         reservation= image.run(
            min_count= numInstances,
            max_count= numInstances,
            security_groups= [self.ec2GroupName],
            key_name= self.ec2KeyName,
            instance_type= self.ec2InstanceType, 
            user_data= ec2BootStrap
         )
      except Exception, e:
         log("error", "could not start intances: %s" % (str(e)))
         return

      for instance in reservation.instances:

         log("debug", "processing reservation")

         # wait for instances to startup   
         # TODO: re-examine this -- must be a better way
         while instance.state != 'running':

            try:
               instance.update()
            except:
               log("warn", "cannot acquire updated instance information")
               pass

            log("debug", "waiting for instance to startup (%s)" % (instance.state))
            sleep(self.sleep)

         log("info", "instance %s running" % (instance.private_dns_name))

         self.instances.update([(instance.private_dns_name, dict(
            id= instance.private_dns_name,
            status= instance.state,
            capacity= 0,
            availability= 0
         ))])

         # wait for instance bootStrap to startup
         # TODO: re-examine this -- must be a better way
         instanceStore= self.instances.get(instance.private_dns_name)
         while instanceStore.get('status') == 'running' and instanceStore.get('capacity', 0) > 0 and instanceStore.get('availability', 0) > 0:
            instanceStore= self.instances.get(instance.private_dns_name)
            log("debug", "waiting for instance %s to bootStrap"  % (instance.private_dns_name))
            sleep(self.sleep)

         log("info", "instance %s bootstrapped and ready" % (instance.private_dns_name))

      log("trace", "end")

   def stopInstances(self, instances= []):
      '''
      Spin down Node instances
      '''

      log("trace", "start")

      for (instanceId, instanceStore) in instances:

         (reservation, )= self.ec2Conn.get_all_instances(filters= {'private_dns_name': instanceId})
         (instance, )= reservation.instances

         self.instances.update([(instanceId, dict(
            id= instance.private_dns_name,
            status= 'stopping', 
            availability= 0,
            capacity= 0
         ))])
         log("info", "stopping instance %s" % (instanceId))

         try:
            instance.stop()
         except Exception, e:
            log("error", "could not stop instance %s: %s" % (instanceId, str(e)))
            continue


         # TODO: re-examin this -- must be a better way
         while instance.state != 'stopped':
            try: 
               instance.update()
            except:
               log("warn", "cannot acquire updated instance information")
               pass

            log("debug", "waiting for instance %s to stop (%s)" % (instanceId, instance.state))
            sleep(self.sleep)

         log("info", "instance %s stopped" % (instanceId))
         instance.terminate()
         self.instances.pop(instanceId)

      log("trace", "end")

   def pipelineMonitor(self):
      '''
      Monitor's the Queues pipeline
      '''

      log("trace", "start")

      def spinDown(instanceStore):
         '''
         Caculates wether to spin down instance based on 
         instance availability and uptime
         '''
        
         # we are fully available (ie, not processing any jobs)
         #fullyAvailable= instanceStore.get('capacity', self.opts.maxProcesses) == instanceStore.get('availability', 0) and instanceStore.get('status') == 'running'
         # seeing if this helps resolve hung instance issue 
         fullyAvailable= instanceStore.get('status') == 'running'

         # if we've been up for an hr (-10 minutes) then shutdown before new billing cycle
         (reservation, )= self.ec2Conn.get_all_instances(filters= {'private_dns_name': instanceStore.get('id')})
         (instance, )= reservation.instances

         currentTime= datetime.utcnow()

         # get the uptime of the instance 
         instanceDelta= currentTime - datetime.strptime(instance.launch_time, '%Y-%m-%dT%H:%M:%S.000Z')
         taskDelta= currentTime - datetime.strptime(instanceStore.get('lastTask', instance.launch_time), '%Y-%m-%dT%H:%M:%S.000Z')

         # if we've been up for 50 minutes think about shutting down -- getting close to the 60 min billing period
         endOfBillingPeriod= ((instanceDelta.days * 86400) + instanceDelta.seconds) >= 3000

         # find out how long we've been fully available for the last 5 minutes consider the instance Idle
         instanceIdle=  ((taskDelta.days * 86400) +  taskDelta.seconds) >= 300

         '''
         if we are fully available, at the end of the billing period and been idel for the last few minutes
         then it's ok to spinDown
         '''
         spinDown= fullyAvailable and endOfBillingPeriod and instanceIdle

         log("info", "instance: %s, instance delta: %s,%s, task delta: %s,%s, available: %s, end of billing period: %s, instance idle: %s, spinDown: %s" % (instance.private_dns_name, instanceDelta.days, instanceDelta.seconds, taskDelta.days, taskDelta.seconds, fullyAvailable, endOfBillingPeriod, instanceIdle, spinDown))

         return spinDown
         

      while self.alive:
   
         try:
   
            qSize= self.pipeline.qsize()
   
            if self.opts.discover:
               '''
               discover instances that may already be running
               because they have either been started outside of 
               dfs or dfs was restarted
               '''
               reservations= self.ec2Conn.get_all_instances()
               for reservation in self.ec2Conn.get_all_instances():
   
                  for instance in filter(lambda instance: self.ec2GroupName in [group.name for group in instance.groups] and instance.state in ('running', 'pending') and instance.id != self.dfsInstance and instance.private_dns_name not in self.instances, reservation.instances):
   
                     self.instances.update([(instance.private_dns_name, dict(
                        id= instance.private_dns_name,
                        status= instance.state,
                        capacity= self.opts.maxProcesses, # assume capcity 
                        availability= 0 # assume unavailable 
                     ))])


            [log("debug", "Instance Store %s: %s" % (instanceId, instanceStore)) for (instanceId, instanceStore) in self.instances.items()]
   
            # get our current capacity and availability
            capacity= sum(map(lambda (instanceId, instanceStore): instanceStore.get('capacity', 0) , self.instances.items()))
            availability= sum(map(lambda (instanceId, instanceStore): instanceStore.get('availability', 0) , self.instances.items()))
   
            if qSize or len(self.store) or len(self.instances) or capacity or availability:
               log("debug", "Queue: %s, Store: %s, Instances: %s, Capacity: %s, Availability: %s" % (qSize, len(self.store), len(self.instances), capacity, availability))
   
            '''
            if we have more jobs in the queue then availbility 
            and we haven't reached our max instance count 
            then spin up what we need to processes the queue
            up unntil the max instance count
            '''
            if qSize > availability and len(self.instances) < self.opts.maxInstances:
               '''
               num instances to spin up is the lesser of the number of instances needed
               to process the job queue comapred to the max number of instances we are allowed
               to spin up where:
   
                  number of instanced needed to process job queue:
                     subtract the number of currently running instances
                     take that number as the max number of instances we are allowed to spin up
                     then take the number of tasks waiting in the queue
                     and divide that by the max number of processes that will run on each instances 
                     and add one -- that will give you the number of instances needed to process
                     the waiting tasks in the queue
   
                  max number of instaces we are allowed to spin up:
                     now take the previously calculated max number of instances we are allowed to spin up
                     and compare that to the number of instances needed to process the waiting tasks in the queue
                     and take the lesser of those two numbers
               '''
               self.startInstances(min(self.opts.maxInstances - len(self.instances), (qSize / self.opts.maxProcesses) + 1))
   
            '''
            if we have more availability then we do jobs in the queue
            then search for instances to spin down and spin then down
            '''
            if qSize < availability:
               self.stopInstances(filter(lambda (instanceId, instanceStore): spinDown(instanceStore), self.instances.items()))

         except EOFError:
            self.connect()
         except IOError:
            self.connect()

         sleep(self.sleep)

      log("trace", "end")

   """
   def stop(self):
      log("info", "STOPPING PIPELINE")
      self.pipelineMonitor.stop()
   """
 
   def run(self):
      '''
      Entry point for the dynamic frequency scaler
      '''
      print 'running DFS', getpid()

      # start the pipeline monitor running
      self.pipelineMonitor.start()

      # get the server process from the base manager and start handling requests
      server= self.get_server()
      server.serve_forever()


class DFS(Daemon):
   '''
   Daemon which startsup the Dynamic Frequency Scaling Manager
   '''
   def __init__(self, opts, pidfile, stdin, stdout, stderr):

      self.opts= opts
      self.alive= True

      self.dfsManager= None

      super(DFS, self).__init__(pidfile= pidfile, stdin= stdin, stdout= stdout, stderr= stderr)

       
   def run(self):
      '''
       Entry point for the Dynamic Frequency Scaling Manginer
      '''
      while self.alive:
         print 'daemonzing dfs manager'
         dfsManager= DFSManager(self.opts)
         dfsManager.run()
         break

   """
   def stop(self):
      if self.dfsManager != None:
         self.dfsManager.stop()
      super(DFS, self).stop()
   """

def parseArgs(argv):
   '''
   Parses command line arguments
   '''

   opt_args= ['start', 'stop', 'restart']

   optParser= OptionParser()

   [ optParser.add_option(opt) for opt in [

      make_option('-f', '--foreground', action='store_true', dest= 'foreground', default= False, help= 'run in foreground'),
      make_option('-q', '--queue', default= '0.0.0.0:50001:impetus', help= 'ip:port:key to bind the queue to'),
      make_option('-d', '--dfs', default= '0.0.0.0:50002:impetus', help= 'ip:port:key to bind the dfs to'),
      make_option('-D', '--dfsInstance', default= 'i-f4fb929f', help= 'static instance dfs is running on'),
      make_option('-Q', '--qInstance', default= 'i-f4fb929f', help= 'static instance queue is running on'),
      make_option('-i', '--maxInstances', default= 5, type=int, help= 'max instances we can spin up'),
      make_option('-m', '--maxProcesses', default= 25, type=int, help= 'max processes per node via bootStrap'),
      make_option('-e', '--ec2', default= 'access:secret:group:key:image:type:bootStrap', help= 'ec2 <access:secret:group:key-name:image:instance-type:bootStrap> infiormation'),
      make_option('-s', '--s3', default= None, help= '<accessKey>:<secretKey>:<bucket> for s3 transport'),
      make_option('-p', '--pidDir', default= path.join(getcwd(), '../pid'), help= 'path to pid directory'),
      make_option('-o', '--logDir', default= path.join(getcwd(), '../log'), help= 'path to log directory'),
      make_option('-n', '--sleep', default= 0.01, type= float, help= 'sleep times for waits'),
      make_option('-c', '--discover', action='store_true', dest='discover', default= False, help= 'auto discover running instances'),
   ]]

   optParser.set_usage('%%prog %s' % ('|'.join(opt_args)))
   opts, args= optParser.parse_args()

   if opts.qInstance == 'None':
      setattr(opts, 'qInstance', None)

   if opts.dfsInstance == 'None':
      setattr(opts, 'dfsInstance', None)

   if not opts.foreground and len(args) < 1:
      optParser.print_usage()
      exit(-1)

   setattr(opts, 'PIDFile', path.join(opts.pidDir, argv[0].replace('.py', '.pid')))

   return args, opts, optParser.print_usage, optParser.print_help

      

if __name__ == '__main__':
   '''
   Main entry point of script
   '''
   # parse command line arguments
   args, opts, opt_usage, opt_help= parseArgs(argv)

   if not path.exists(opts.pidDir):
      log.debug('directory %s does not exist' % (opts.pidDir))
      try:
         log('debug', 'creating directory %s' % (opts.pidDir))
         makedirs(opts.pidDir)
      except:
         log('error', 'could not create directory %' % (opts.pidDir))
         exit(-1)

   if not path.exists(opts.logDir):
      log('debug', 'directory %s does not exist' % (opts.logDir))
      try:
         log('debug', 'creating directory %s' % (opts.logDir))
         makedirs(opts.logDir)
      except:
         log('error', 'could not create directory %s' % (opts.pidDir))
         pass


   if not opts.foreground:

      # setup std i/o
      si= path.join(opts.logDir, 'dfs-stdin.log')
      so= path.join(opts.logDir, 'dfs-stdout.log')
      se= path.join(opts.logDir, 'dfs-stderr.log')

      dfs= DFS(opts, pidfile= opts.PIDFile, stdin= si, stdout= so, stderr= se)
      if 'start' in args:
         print "starting"
         dfs.start()
      elif 'stop' in args:
         print "stopping"
         dfs.stop()
      elif 'restart' in args:
         print "restarting"
         dfs.restart()
      else:
         opt_usage()
         exit(2)

   else:

      dfsManager= DFSManager(opts)
      dfsManager.run()
 
 
   exit(0)
