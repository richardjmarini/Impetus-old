#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
   Author: Richard J. marini (richardjmarini@gmail.com)
   Date: 10/4/2013
   Desc: High level client side library/package to be used to interface with the 
   impetus distributed processing system. Simplifies the Task package.
'''

from sys import stdout, stderr
from datetime import datetime
from operator import attrgetter
from time import sleep
from os import path, pardir, makedirs
from types import FunctionType
from codecs import open as utf8open
from threading import Thread, Lock, currentThread
from json import dumps

from task import Task


__thread_order__= 0

class Bot(Task):

   node= staticmethod

   def __init__(self, queue, taskId= None, s3= None, taskDir= None):
 
      super(Bot, self).__init__(queue= queue, taskId= taskId, s3= s3, taskDir= taskDir)
      
      self.errors= {}
      self.ready= {}
      self.threads= []
      self.counter= {}

      self._current_thread= None
      self._lock= Lock()

      self.taskDir= path.join(taskDir, self.getTaskId())

      try:
         makedirs(self.taskDir)
      except Exception, e:
         print >> stderr, str(e), "...ignoring"
         pass

   @staticmethod
   def startup(process):

      global __thread_order__

      def _process(self):
        
         process(self)

      _process.order= __thread_order__
      __thread_order__+= 1
      return _process

   @staticmethod
   def cleanup(clean):

      global __thread_order__

      def _cleanup(self, ready, errors, counter):
         clean(self, ready, errors, counter)

      _cleanup.order= __thread_order__
      __thread_order__+= 1
      return _cleanup


   @staticmethod
   def process(process):

      def _process(self):

         current_thread= currentThread()
         previous_thread= current_thread.previous_thread

         while True:

            self.thread_regulator(current_thread, previous_thread)

            sub_tasks= self.getTask(tag= current_thread.name)

            ready= filter(lambda result: result.get('status') == 'ready', sub_tasks)
            errors= filter(lambda result: result.get('status') == 'error', sub_tasks)
          
            for results in ready:
               self.ready[current_thread.name].write(dumps(results) + '\n')
               self.killSubTask(int(results.get('processId').split('.')[1]))

            for error in errors:
               self.errors[current_thread.name].write(dumps(error) + '\n')
               self.killSubTask(int(error.get('processId').split('.')[1]))

            if len(ready) or len(errors):
               process(self, ready, errors)

            self.update_counter(current_thread.name, 'processed', len(ready) + len(errors))
            self.show_counter(current_thread)
            
            if len(sub_tasks) == 0 and previous_thread != None and previous_thread.is_alive() == False:
               print "%s %s completed" % (datetime.utcnow(), current_thread.name)
               break
            
            sleep(0.01)

      global __thread_order__
      _process.order= __thread_order__
      __thread_order__+= 1
      return _process

   def thread_regulator(self, current_thread, previous_thread):
    
      stall_time= 1
      while self._current_thread == current_thread:
         sleep(stall_time)
         stall_time+= 1
         if stall_time >= 10:
            break

         if current_thread.name == self.threads[-1].name and  previous_thread != None and previous_thread.is_alive() == False:
            with self._lock:
               self._current_thread= self.threads[0]

      with self._lock:
         self._current_thread= current_thread

   def forkTask(self, taskCode, taskArgs, processor= None, transport= 'store'):

      current_thread= currentThread()
      next_thread_name= processor if processor else current_thread.next_thread.name
      super(Bot, self).forkTask(taskCode, taskArgs, next_thread_name, transport)

      self.update_counter(current_thread.name, 'forked', 1)

   def update_counter(self, name, counter_type, count):
 
      with self._lock:
         counter= self.counter.get(name, {'forked': 0, 'processed': 0})
         counter.update([(counter_type, counter.get(counter_type) + count)])
         self.counter.update([(name, counter)])
  
   def show_counter(self, current_thread):

      msg= ""

      with self._lock:
         for thread in self.threads:
            counter= self.counter.get(thread.name, {'forked': 0, 'processed': 0})
            msg+= "%s %s/%s -> " % (thread.name, counter.get('forked'), counter.get('processed'))  

      print "%s %s via %s" % (datetime.utcnow(), msg[:-4], current_thread.name)
      
   def run(self):
     
      threads= filter(lambda (tag, target): type(target) == FunctionType and target.__name__ == '_process', self.__class__.__dict__.items())
      for tag, target in sorted(threads, key= lambda (tag, target): target.order):
         self.threads.append(Thread(target= target, name= tag, args= (self, )))
         self.errors.update([(tag, utf8open(path.join(self.taskDir, '.'.join([tag, 'err'])), 'ab+'))])
         self.ready.update([(tag, utf8open(path.join(self.taskDir, '.'.join([tag, 'ok'])), 'ab+'))])

      self._current_thread= self.threads[0]
      for i in range(len(self.threads)):
         setattr(self.threads[i], 'previous_thread', self.threads[i-1] if i > 0 else None)
         setattr(self.threads[i], 'next_thread', self.threads[i+1] if i < len(self.threads) - 1 else None)

      [thread.start() for thread in self.threads]
      [thread.join() for thread in self.threads]

      cleanup= filter(lambda (tag, target): type(target) == FunctionType and target.__name__ == '_cleanup', self.__class__.__dict__.items())
      [target(self, self.ready, self.errors, self.counter) for tag, target in sorted(cleanup, key= lambda (tag, target): target.order)]
