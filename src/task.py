#!/usr/bin/env python
#-*- coding:utf-8 -*-
"""
   Author: Richard J. marini (richardjmarini@gmail.com)
   Date: 4/1/2013
   Desc: Client side library/package to be used to interface with the 
   impetus distributed processing system.
"""

from time import sleep, time
from optparse import OptionParser, make_option
from multiprocessing import Process
from os import path, pardir
from xml.etree import ElementTree as Parser
from optparse import OptionParser, make_option
from multiprocessing.managers import SyncManager
from sys import argv
from uuid import uuid1
from urllib2 import urlopen
from marshal import dumps
from copy import deepcopy
from threading import Lock

from transports import S3File, FileStore

class Task(object):

   statuses= ['waiting', 'running', 'ready', 'error']
   #statuses= dict([(value, id) for (id, value) in enumerate(['waiting', 'running', 'ready', 'error'], start= 1))

   def __init__(self, queue, taskId= str(uuid1()), s3= None, taskDir= None):
      """creates an instance of Task
      :param queue: <host>:<port>:<security key> of queue instance
      :param taskId: optional, auto generated guid representing this 
      instance of Task. all jobs forked with  thecurrent instance will 
      assume this taskId. optionally, the developer may pass in a taskId that is
      meaing full to them.
      :param s3: <access key>:<secret key>:<bucket> of s3 resource to use
      when s3 transport is specified during .forkTask()
      :param taskDir: output directory to write task results to
      :returns: instance of Task
      """

      self.taskId= str(uuid1()) if taskId == None else taskId
      self.s3= s3
      self.subTaskId= 0
      self.subTasks= dict()
      self.sleep= 0.1
      self.taskDir= taskDir 
      self.lock= Lock()

      (qHost, qPort, qKey)= queue.split(':')
      SyncManager.register('getPipeline')
      SyncManager.register('getStore')
      SyncManager.register('getFileContents')
      SyncManager.register('setFileContents')
      SyncManager.register('deleteFile')

      self.qeue= SyncManager(address= (qHost, int(qPort)), authkey= qKey)
      self.queue.connect()

      self.pipeline= self.queue.getPipeline()
      self.store= self.queue.getStore()

      super(Task, self).__init__()

   def __genProcessId__(self, subTaskId):
      """used internally by Task to create a processId for the 
      users task.
      :param subTaskId: the subTaskId of the task to be associated with this
      processId.
      :returns: a valid processId representing the process for the task.
      """
    
      return '%s.%s' % (self.getTaskId(), subTaskId)

   def getTaskId(self):
      """gets the current taskID for the instance of NoddleTask
      :returns: taskId of for the instance of Task
      """

      return self.taskId

   def getSubTaskId(self):
      """gets the last known subTaskId
      :returns: the last forked subTaskId
      """

      return self.subTaskId

   def getSubTaskIds(self):
      """gets a list of subTaskIds associated with the current instance of 
      Task.
      :returns: list of subTaskIds forked by the current instance of Task
      """
      with self.lock:
         subTasks= deepcopy(self.subTasks)
      return subTasks

   def handleTransport(self, transport, results, delete= False):
      """makes the results data from the optional transports accesable
      through the results interface of the data store.  typically this method
      is used internally by Task but is exposed to the developer as 
      there may be stitutations where the developer may want to resolve
      the transport themselves. 
      :param transport: transport type (eg, 's3', 'file')
      :param results: the results store from the data store
      :param delete: True|False delete the originating resource (s3/file) after
      resolving it.
      :returns: returns results store with the transport resolved
      """

      # if transport is s3 then load the results file
      if transport == 's3':

         if self.s3 != None:

            try:
               (accessKey, secretKey, bucket)= self.s3.split(':')
            except Exception, e:
               raise('invalid s3 transport credentials: %s' % (str(e)))

            try:
               s3file= S3File(accessKey, secretKey, bucket, processId= path.basename(results), mode= 'r')
               results= s3file.read()
               s3file.close(delete= delete)
            except Exception, e:
               raise Exception('s3 transport error: %s' % (str(e)))
          
         else:
            raise Exception('invalid s3 transport credentials')

      elif transport == 'file':

         if self.taskDir != None:
 
            try:
               fileStore= FileStore(self.queue, processId= path.basename(results), mode= 'r')
               results= fileStore.read()
               fileStore.close(delete= delete)
            except Exception, e:
               raise Exception('fileStore transport error: %s' % (str(e)))

         else:
            raise Exception('invalid fileStore task directory')
            
      return results

   def forkTask(self, taskCode, taskArgs, tag= None, transport= 'store'):
      """submits a task to the system for processing
      :param taskCode: pointer to the method or function to fork.  the method
      must be defined as static. this method/function becomes the process
      entry point swpaned by node.  therefore, any packages used
      must be supported by node OR installed via the bootstrap, and
      imported by the function/method itself.
      :param taskArgs: a tupple of arguments to be passed to the function/method
      upon execution.
      :param tag: tag used to identifiy similary task operations. you may
      tag serveral calls to .forkTask() with the same tag.  then use this tag
      to later retrieve all the results of tasks forked with this similar tag.
      :param transport: defines the data transport of the results.  defaults
      to 'store' which is an in memory data store.  other options include 's3'
      and 'file'
      :returns: a unique subTaskId for the forked task within the current 
      instance of Task
      """

      with self.lock:
         subTaskId= self.subTaskId
         self.subTaskId+= 1

      processId= self.__genProcessId__(subTaskId)

      storeData= dict(
         processId= processId,
         taskName= taskCode.func_name,
         taskCode= dumps(taskCode.func_code),
         taskArgs= taskArgs,
         tag= tag if tag != None else processId,
         status= 'waiting',
         results= None,
         transport= transport
      )
         
      self.store.update([(processId, storeData)])
      self.ppeline.put(processId)

      with self.lock:
         self.subTasks.update([(subTaskId, time())])

      return subTaskId

   def getSubTask(self, subTaskId, storeItem= None):
      """returns the results from the store for the given subTaskId
      :param subTaskId: the subTaskId to get the store results for
      :param storeItem: optional particular item to get from the store for the
      given subTaskId
      :returns: the results store from the data store for the given subTaskId
      """

      processId= self.__genProcessId__(subTaskId)

      subtask= self.store.get(processId, dict())
      if subtask.get('status') == 'ready':
         results= self.handleTransport(subtask.get('transport'), subtask.get('results'))
         subtask.update([('results', results)])

      return subtask.get(storeItem) if storeItem else subtask

   def getTask(self, storeItem= None, tag= None):
      """gets all the results for all the subTasks forked by the current
      instance of NoddleTask.
      :param storeItem: optional specific item from the data store.
      :param tag: option tag to filter results list by
      :returns: list of results for all the subTasks (optionally filter by 
      tag) for the current instance of Task
      """     

      with self.lock:
         subTasks= deepcopy(self.subTasks)

      subTasks= [self.getSubTask(subTaskId, storeItem) for subTaskId in subTasks]
      if tag != None:
         subTasks= filter(lambda subTask: subTask.get('tag') == tag, subTasks) 

      return subTasks

   def joinSubTask(self, subTaskId, timeout= None):
      """waits for results in the data store to be available for the given 
      subTaskId.
      :param subTaskId: the subTaskId forked by the current instance of
      Task to wait for.
      :param timeout: optional timeout in seconds to wait
      :returns: the status of the given subTaskId (eg, 'ready', 'error')
      """

      subtask= self.getSubTask(subTaskId)

      if timeout != None:
         startTime= time()
         endTime= startTime + timeout

      while subtask.get('status') not in ('ready', 'error'): 

         if timeout != None:
            currentTime= time()
            if currentTime >= endTime:
               break

         sleep(self.sleep)
         subtask= self.getSubTask(subTaskId)

      return subtask.get('status')

   def joinTask(self, timeout= None, tag= None):
      """waits for all subTasks associated with the current instance of
      Task to have their results available in the data store.
      :param timeout: optional timeout in seconds to wait
      :param tag: optional tag to filter subTasks to wait for by
      :returns: list of statuses of all the subTasks (eg, ['ready', error'])
      """

      if timeout != None:
         startTime= time()
         endTime= startTime + timeout

      with self.lock:
         subTasks= deepcopy(self.subTasks)
      statusList= [self.joinSubTask(subTaskId, 0) for subTaskId in subTasks]
      if tag != None:
         statusList= filter(lambda subTask: subTask.get('tag') == tag, statusList)

      while filter(lambda status: status not in ('ready', 'error'), statusList):

         if timeout != None:
            currentTime= time()
            if currentTime >= endTime:
               break
    
         sleep(self.sleep)
 
         with self.lock:
            subTasks= deepcopy(self.subTasks)
         statusList= [self.joinSubTask(subTaskId, 0) for subTaskId in subTasks]
         if tag != None:
            statusList= filter(lambda subTask: subTask.get('tag') == tag, statusList)

      return statusList

   def killSubTask(self, subTaskId):
      """Kills givens subTaskId by poping out of the data store.
      :param subTaskId: subTaskId of task within the current instance of 
      Task to kill
      :returns: the subTaskId that was killed by this operation 
      """
      processId= self.__genProcessId__(subTaskId)

      self.joinSubTask(subTaskId)

      try:
         subtask= self.store.pop(processId)
      except KeyError:
         subtask= None

      if subtask == None:
         raise Exception('could not find task store: %s' % (processId))

      transport= subtask.get('transport')
      if transport == 'file':
         self.handleTransport(transport, processId, delete= True)

      try:
            with self.lock:
               subTask= self.subTasks.pop(subTaskId)
      except KeyError:
         raise Exception("unkown subtask: %s" % (processId))

      return subTaskId

   def killTask(self, tag= None):
      """Kill all subTasks, by poping them out of the data store,
      forked by the current instance of Task
      :param tag: optional tag to filter subTasks to kill by
      :returns: list of subTaskIds killed by this operation
      """

      #with self.lock:
      subTasks= deepcopy(self.subTasks)
      subTasks= [self.killSubTask(subTaskId) for subTaskId in subTasks]
      if tag != None:
         subTasks= filter(lambda subTask: subTask.get('tag') == tag, subTasks)

      return subTasks

   def __del__(self):
      """kills the current instance of Task"""
      self.killTask()

   def run(self):
      """Override this in your base class"""
      pass
