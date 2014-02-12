#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
   Author: Richard J. marini (richardjmarini@gmail.com)
   Date: 4/1/2013
   Name: Queue
   Desc: Impliments distributed processing queue 
'''

from datetime import datetime
from glob import glob
from os import path, getcwd, pardir, makedirs, remove
from sys import exit, argv, stderr, stdout, stdin
from multiprocessing.managers import SyncManager, DictProxy, Value, Array
from multiprocessing import JoinableQueue, Lock
from optparse import OptionParser, make_option
from logging import basicConfig

from daemon import Daemon

class MemcacheStore(DictProxy):

   def __init__(self, host, port):
      from memcache import Client
      self.mc= Client(['%s:%s' % (host, port)])

   def update(self, updates):
      for update in updates:
         (processId, data)= update
         self.mc.set(processId, data)

   def get(self, processId, default= None):
      data= self.mc.get(processId)
      if data == None:
         return default
      return data

   def pop(self, processId):
      data= self.mc.get(processId)
      self.mc.delete(processId)
      return data
      if data == None:
         return default

   def __len__(self):
      return int(self.mc.get_stats()[0][1].get('curr_items'))


class SharedQueue(SyncManager):
   '''
   Impliments shared queue of task to process
   '''

   def __init__(self, opts):
      '''
      Initializes shared queue and registers public handlers
      '''

      self.opts= opts
      self.qPipeline= JoinableQueue()

      print 'using %s datastore' % (self.opts.storeType)
      if self.opts.storeType == 'memcache': 
         self.qStore= MemcacheStore(self.opts.mcHost, self.opts.mcPort)
      else:
         self.qStore= dict()
           

      self.lock= Lock()
       
      # register handlers with the base manager
      self.register('getPipeline', callable= lambda: self.qPipeline)
      self.register('getStore', callable= lambda: self.qStore, proxytype= DictProxy)
      self.register('setFileContents', callable= self.setFileContents)
      self.register('getFileContents', callable= self.getFileContents)
      self.register('deleteFile', callable= self.deleteFile)

      # create the manager instance and bind it to a ipaddr:port
      (qHost, qPort, qKey)= self.opts.queue.split(':')
      super(SharedQueue, self).__init__(address= (qHost, int(qPort)), authkey= qKey)

   def setFileContents(self, processId, results):
  
      (taskId, subTaskId)= processId.split('.')
      taskDir= path.join(self.opts.taskDir, taskId)
      try:
         makedirs(taskDir)
      except:
         pass

      resultsFile= path.join(taskDir, processId)
      fh= open(resultsFile, 'wb')
      fh.write(results)
      fh.close()

   def getFileContents(self, processId):

      (taskId, subTaskId)= processId.split('.')
      taskDir= path.join(self.opts.taskDir, taskId)
      resultsFile= path.join(taskDir, processId)
      fh= open(resultsFile, 'rb')
      results= fh.read()
      fh.close()
      return Array('c', results)

   def deleteFile(self, processId):

      (taskId, subTaskId)= processId.split('.')
      taskDir= path.join(self.opts.taskDir, taskId)
      resultsFile= path.join(taskDir, processId)
      
      try:
         remove(resultsFile)
      except:
         pass

      if len(glob(path.join(taskDir, '*'))) == 0:
         try:
            rmdir(taskDir)
         except:
            pass

   def run(self):
      '''
      Entry point for the shared queue -- startsup the queue server
      '''

      print "shared queue running", datetime.now()

      # get the server process from the base manager and start handling requests
      server= self.get_server()
      server.serve_forever()


class Queue(Daemon):
   '''
   Daemon which startsup the shared Queue
   '''
   def __init__(self, opts, pidfile, stdin, stdout, stderr):

      self.opts= opts
      self.alive= True
      super(Queue, self).__init__(pidfile= pidfile, stdin= stdin, stdout= stdout, stderr= stderr)


   def run(self):
      '''
       Entry points to Shared Q daemon instance
      '''
      while self.alive:
         print 'daemonzing queue'
         sharedQueue= SharedQueue(self.opts)
         sharedQueue.run()
         break


def parseArgs(argv):
   '''
   Parses command line arguments
   '''

   opt_args= ['start', 'stop', 'restart']

   optParser= OptionParser()

   [ optParser.add_option(opt) for opt in [
      make_option('-f', '--foreground', action='store_true', dest= 'foreground', default= False, help= 'run in foreground'),
      make_option('-q', '--queue', default= '0.0.0.0:50001:impetus', help= 'ip:port:key to bind the queue to'),
      make_option('-s', '--storeType', default= 'dict', help= 'dict|memcache:host:port'),
      make_option('-p', '--pidDir', default= path.join(getcwd(), '../pid'), help= 'path to pid directory'),
      make_option('-o', '--logDir', default= path.join(getcwd(), '../log'), help= 'path to log directory'),
      make_option('-d', '--taskDir', default= path.join(getcwd(), '../tasks'), help= 'path to task directory')
   ]]

   optParser.set_usage('%%prog %s' % ('|'.join(opt_args)))

   opts, args= optParser.parse_args()
   if not opts.foreground and len(args) < 1:
      optParser.print_usage()
      exit(2)

   if 'memcache' in opts.storeType:
      try:
         (storeType, mcHost, mcPort)= opts.storeType.split(':')
         setattr(opts, 'storeType', storeType)
         setattr(opts, 'mcHost', mcHost)
         setattr(opts, 'mcPort', mcPort)
      except:
         print 'invalid memcache parameters memcache:<host>:<port> in --storeType'
         optParser.print_help()
         exit(2)
   

   setattr(opts, 'PIDFile', path.join(opts.pidDir, argv[0].replace('.py', '.pid')))

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

      si= path.join(opts.logDir, 'queue-stdin.log')
      so= path.join(opts.logDir, 'queue-stdout.log')
      se= path.join(opts.logDir, 'queue-stderr.log')

      queue= Queue(opts, pidfile= opts.PIDFile, stdin= si, stdout= so, stderr= se)
      if 'start' in args:
         print "starting"
         queue.start()
      elif 'stop' in args:
         print "stopping"
         queue.stop()
      elif 'restart' in args:
         print "restarting"
         queue.restart()
      else:
         opt_usage()
         exit(2)

   else:
      queue= SharedQueue(opts)
      queue.run()
  
   exit(0)
