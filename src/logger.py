#!/usr/bin/env python
#-*- coding:utf-8 -*-

from datetime import datetime
from traceback import extract_stack
from sys import stdin, stdout, stderr, exit, argv

def log(lvl, msg):
   '''
   Formats and prints message to appropriate i/o stream.
   TODO: perhaps look into replace with logging module
   '''

   (
      procName,
      lineNum,
      funcName,
      funcName2
   )= extract_stack()[len(extract_stack())-2]

   msg= "%s, %s, %s, %s, %s, %s\n" % (
      datetime.now(),
      lvl,
      procName,
      funcName,
      lineNum,
      msg
   )

   ioStream= dict(error= stderr).get(lvl, stdout)
   print >> ioStream, msg,
   ioStream.flush()


def logS3Transfer(numBytes, totalBytes):
   '''
   Formats and prints transfer status messages
   '''

   pctDone= (numBytes / float(totalBytes)) * 100

   log("debug", "transfered %s bytes of %s bytes %0.2f%% done" % (numBytes, totalBytes, pctDone))

