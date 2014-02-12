#!/usr/bin/env python
#-*- coding:utf-8 -*-

from os import path, getcwd, fork, chdir, setsid, umask, getpid, dup2, remove, kill, makedirs, pardir, stat, rename

from signal import SIGTERM
from time import sleep
from atexit import register
from sys import exit, stderr, stdin, stdout

DEFAULT_IO= '/dev/null'

class Daemon(object):
   '''
   Damonizes base object
   '''

   def __init__(self, pidfile, stdin= DEFAULT_IO, stdout= DEFAULT_IO, stderr= DEFAULT_IO):
      '''
      Initializes daemon
      '''
      self.stdin= stdin
      self.stdout= stdout
      self.stderr= stderr
      self.pidfile= pidfile
      super(Daemon, self).__init__()

   def fork(self):
      '''
      Forks off the the process into the background
      '''
      try:
         pid= fork()
         if pid > 0:
            exit(0)
      except OSError, e:
         exit(1)

   def daemonize(self):
      '''
      Forks then sets up the I/O stream for the daemon 
      '''
      self.fork()

      chdir(getcwd())
      setsid()
      umask(0)
  
      self.fork()
      stdout.flush()
      stderr.flush()

      si= file(self.stdin, 'w+')
      so= file(self.stdout, 'a+')
      se= file(self.stderr, 'a+', 0)

      dup2(si.fileno(), stdin.fileno())
      dup2(so.fileno(), stdout.fileno())
      dup2(se.fileno(), stderr.fileno())

      register(self.delPID)
      self.setPID()

   def setPID(self):
      '''
      Creates PID file for the current daemon on the filesystem
      '''
      pid= str(getpid())
      fh= open(self.pidfile, 'w')
      fh.write(pid)
      fh.close()

   def delPID(self):
      '''
       Removes the PID file from the filesystem
      '''
      remove(self.pidfile)

   def getPID(self):
      '''
      Reads the PID from the filesystem
      '''
      try:
          pid= int(open(self.pidfile, 'r').read())
      except IOError, e:
          pid= None

      return pid

   def start(self):
      '''
      Startup the daemon process
      '''
      pid= self.getPID()
      if pid:
         exit(1)

      self.daemonize()
      self.run()

   def stop(self):
      '''
      Stops the daemon process
      '''
      pid= self.getPID()
      if not pid:
         return

      try:
         kill(pid, SIGTERM)
         sleep(0.1)
         self.delPID()
      except OSError, e:
         if str(e).find('No such process') > 0:
            if path.exists(self.pidfile):
               self.delPID()
         else:
            exit(1)

   def restart(self):
      '''
      Restarts the daemon process
      '''
      self.stop()
      self.start()

   def run(self):
      '''
      Overridden in base class
      '''
      pass
