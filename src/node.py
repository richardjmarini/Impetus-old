#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
   Author: Richard J. marini (richardjmarini@gmail.com)
   Date: 4/1/2013
   Name: Processing Node
   Desc: Responsible for concurrently processing jobs off the queue 
   on a single processing node instance 
'''

import random
from uuid import uuid1
from datetime import datetime
from os import path, getcwd, pardir, makedirs
from sys import exit, argv, stderr, stdout, stdin, exc_info
from multiprocessing.managers import SyncManager, Process
from multiprocessing import JoinableQueue
from Queue import Empty
from optparse import OptionParser, make_option
from logging import basicConfig
from time import sleep
from socket import getfqdn
from marshal import loads
from types import FunctionType
from traceback import extract_tb
from atexit import register
from re import search, match, sub, findall

from daemon import Daemon
from transports import S3File, FileStore

class Worker(Process):
   
   def __init__(self, opts, id, availability):

      super(Worker, self).__init__()
      self.opts= opts
      self.id= id
      self.availability= availability

      self.connect()

      self.alive= True
      self.sleep= self.opts.sleep

   def connect(self):
      (qHost, qPort, qKey)= self.opts.queue.split(':')
      self.queue= SyncManager(address= (qHost, int(qPort)), authkey= qKey)
      self.queue.connect()
      self.pipeline= self.queue.getPipeline()
      self.store= self.queue.getStore()

      # register with DFS
      self.dfs = None
      self.instances= dict()
      if self.opts.dfs != None:
         SyncManager.register('getInstances')
         (dHost, dPort, dKey)= self.opts.dfs.split(':')
         self.dfs= SyncManager(address= (dHost, int(dPort)), authkey= dKey)
         self.dfs.connect()
         self.instances= self.dfs.getInstances()


   def handleTransport(self, processId, transport, results):
      '''
      Handles requested transport types
      '''

      if transport == 's3' and self.opts.s3 != None:

         try:
            (accessKey, secretKey, bucket)= self.opts.s3.split(':')

            s3file= S3File(
               accessKey= accessKey, 
               secretKey= secretKey,
               bucket= bucket,
               processId= processId,
               mode= 'w'
            )

            s3file.write(results)
            results= s3file.getName()
            s3file.close()
            transport= 's3'

         except Exception, e:
            print >> stderr, "s3 transport failure using data store instead: %s" % (str(e))

      elif transport == 'file' and self.opts.taskDir != None:

         try:
            fileStore= FileStore(proxy= self.queue, processId= processId, mode= 'w')
            fileStore.write(results)
            results= fileStore.getName()
            fileStore.close()
            transport= 'file'

         except Exception, e:
            print >> stderr, "fileStore transport failure using data store instead: %s" % (str(e))

      else:
         transport= 'store'
     
      return (transport, results)


   def run(self):

      startTime= datetime.now()

      #ss= random.randint(1, 30)
      #print self.id, "working for", ss
      #sleep(ss)

      while self.alive:

         try:
            taskId= self.pipeline.get(False)
            storeData= self.store.get(taskId)
            if storeData != None:

               status= storeData.get('status')
               taskName= storeData.get('taskName')
               taskCode= storeData.get('taskCode')
               taskArgs= storeData.get('taskArgs')
               tag= storeData.get('tag')
               results= storeData.get('results', None)
               processId= storeData.get('processId')
               transport= storeData.get('transport')

               self.store.update([(taskId, dict(
                  status= 'running',
                  taskName= taskName,
                  taskCode= taskCode,
                  taskArgs= taskArgs,
                  tag= tag,
                  results= results,
                  processId= processId,
                  transport= transport
               ))])

               self.instances.update([(self.id, dict(
                  id= self.id,
                  status= 'running',
                  capacity= self.opts.maxProcesses,
                  availability=  self.availability,
                  lastTask=  datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.000Z')
               ))])

               print "running task", taskId, taskName

               try:

                  handler= FunctionType(
                     loads(taskCode),
                     globals(),
                     taskName
                  )
                  results= handler(taskArgs)
                  (transport, results)= self.handleTransport(processId, transport, results)
                  status= 'ready'

               except Exception, e:
                  (fileName, lineNum, funcName, statement)= extract_tb(exc_info()[2])[-1]
                  results= {
                     'error': str(e),
                     'taskName': taskName,
                     'lineNum': lineNum,
                     'statement': statement
                  }

                  print >> stderr, 'WARNING', results
                  transport= 'store'
                  status= 'error'

               self.store.update([(taskId, dict(
                  status= status,
                  taskName= taskName,
                  #taskCode= taskCode,
                  taskArgs= taskArgs,
                  tag= tag,
                  results= results,
                  processId= processId,
                  transport= transport
               ))])

            else:
               print "Could not find data for task %s assuming task killed" % (taskId)

            self.pipeline.task_done()

         except Empty:
            self.alive= False
         except EOFError:
            self.connect()
         except IOError:
            self.connect()
         except Exception, e:
            print >> stderr, 'ERROR processing task %s' % (str(e))
            self.alive= False

         sleep(self.sleep)

      endTime= datetime.now()
      runTime= endTime - startTime
      


class Manager(object):

   def __init__(self, opts):

      self.opts= opts
      super(Manager, self).__init__()

      self.sleep= self.opts.sleep
      self.alive= True
      self.workers= dict()

      self.connect()

      '''
      The fully qualified domain name for the aws ec2 instance
      should match what the instance private_dns_name is 
      '''
      self.id= getfqdn()

   def connect(self):

      # register with queue
      SyncManager.register('getPipeline')
      SyncManager.register('getStore')
      SyncManager.register('setFileContents')
      SyncManager.register('getFileContents')
      SyncManager.register('deleteFile')

      (qHost, qPort, qKey)= self.opts.queue.split(':')
      self.queue= SyncManager(address= (qHost, int(qPort)), authkey= qKey)
      self.queue.connect()
      self.pipeline= self.queue.getPipeline()
      self.store= self.queue.getStore()

      # register with dfs
      self.dfs = None
      self.instances= dict()
      if self.opts.dfs != None:
         SyncManager.register('getInstances')
         (dHost, dPort, dKey)= self.opts.dfs.split(':')
         self.dfs= SyncManager(address= (dHost, int(dPort)), authkey= dKey)
         self.dfs.connect()
         self.instances= self.dfs.getInstances()

   def run(self):

      while self.alive:
     
         try:  
            # stop tracking dead workers
            [self.workers.pop(pid) for (pid, worker) in self.workers.items() if not worker.is_alive()]

            instanceStore= self.instances.get(self.id, dict())

            # update dfs worker availability
            availability= self.opts.maxProcesses - len(self.workers) 
            self.instances.update([(self.id, dict(
               id= self.id,
               status= 'running',
               capacity= self.opts.maxProcesses,
               availability= availability,
               lastTask= instanceStore.get('lastTask', datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.000Z')
)
            ))])

            print "========================================================"  
            print "Queue:", self.pipeline.qsize()
            print "Store:", len(self.store)
            print "Capacity:", self.opts.maxProcesses
            print 'Workers:', len(self.workers)
            print "Availability:", self.opts.maxProcesses - len(self.workers)
            print "--------------------------------------------------------"  
          
            # create workers
            for i in range(min(self.pipeline.qsize(), self.opts.maxProcesses - len(self.workers))):
               worker= Worker(self.opts, self.id, availability)
               worker.start()
               self.workers[worker.pid]= worker
         except EOFError:
            self.connect()
         except IOError:
            self.connect()
 
         sleep(self.sleep)

      # if manager is shutting down -- then wait for workers to finish
      print "manager shutting down"
      map(lambda (pid, worker): worker.join(), self.workers.items())

   def stop(self):

      print "de-registering with dfs -- all workers down"

      ''' 
      tell dfs are are shutting down and have no capacity/availabilty
      if dfs doesn't know who we are then create a default stub
      '''
      self.instances.update([(self.id, dict(
         id= self.id,
         status= 'running',
         capacity= 0,
         availability= 0
      ))])

      self.alive= False


class Node(Daemon):

   def __init__(self, opts, pidfile, stdin, stdout, stderr):

      self.opts= opts
      self.alive= True
      self.manager= Manager(self.opts)
      super(Node, self).__init__(pidfile= pidfile, stdin= stdin, stdout= stdout, stderr= stderr)

   def run(self):

      while self.alive:
         print "daemonizing manager"
         self.manager.run()
         break

   def stop(self):
      if self.manager != None:
         print "terminating manager -- killing all workers"
         self.manager.stop()

      print "node down"
      super(Node, self).stop()


def parseArgs(argv):
   '''
   Parses command line arguments
   '''

   opt_args= ['start', 'stop', 'restart']

   optParser= OptionParser()

   [ optParser.add_option(opt) for opt in [
      make_option('-f', '--foreground', action='store_true', dest= 'foreground', default= False, help= 'run in foreground'),
      make_option('-q', '--queue', default= 'ec2-184-72-140-129.compute-1.amazonaws.com:50001:impetus', help= 'queue ip:port:key'),
      make_option('-d', '--dfs', default= 'ec2-184-72-140-129.compute-1.amazonaws.com:50002:impetus', help= 'dfs ip:port:key'),
      make_option('-s', '--s3', default= None, help= '<accessKey>:<secretKey>:<bucket> for s3 transport'),
      make_option('-p', '--pidDir', default= path.join(getcwd(), '../pid'), help= 'path to pid directory'),
      make_option('-o', '--logDir', default= path.join(getcwd(), '../log'), help= 'path to log directory'),
      make_option('-t', '--taskDir', default= path.join(getcwd(), '../tasks'), help= 'path to task directory'),
      make_option('-m', '--maxProcesses', default= 25, type= int, help= 'path to pid directory'),
      make_option('-n', '--sleep', default= 0.01, type= float, help= 'sleep time for waits')
   ]]

   optParser.set_usage('%%prog %s' % ('|'.join(opt_args)))

   opts, args= optParser.parse_args()
   if not opts.foreground and len(args) < 1:
      optParser.print_usage()
      exit(-1)

   setattr(opts, 'PIDFile', path.join(opts.pidDir, argv[0].replace('.py', '.pid')))

   if opts.dfs == 'None':
      opts.dfs = None

   return args, opts, optParser.print_usage, optParser.print_help



if __name__ == '__main__':
   '''
   Main entry point of script
   '''

   # parse commadn line arguments
   args, opts, opt_usage, opt_help= parseArgs(argv)

   try:
      makedirs(opts.pidDir)
   except:
      pass

   try:
      makedirs(opts.logDir)
   except:
      pass

   if not opts.foreground:

      si= path.join(opts.logDir, 'node-stdin.log')
      so= path.join(opts.logDir, 'node-stdout.log')
      se= path.join(opts.logDir, 'node-stderr.log')

      node= Node(opts, pidfile= opts.PIDFile, stdin= si, stdout= so, stderr= se)
      if 'start' in args:
         print "starting"
         node.start()
      elif 'stop' in args:
         print "stopping"
         node.stop()
      elif 'restart' in args:
         print "restarting"
         node.restart()
      else:
         opt_usage()
         exit(2)

   else:

      node= Manager(opts)
      node.run()

   exit(0)

