#!/usr/bin/env python
#-*- coding:utf-8 -*-
'''
   Author: Richard J. marini (richardjmarini@gmail.com)
   Date: 4/1/2013
   Name: transports
   Desc: Data transport methods from the output of the nodes back to the client
'''

from datetime import datetime
from tempfile import NamedTemporaryFile
from boto.s3.connection import S3Connection
from boto.s3.key import Key
#from boto.s3.cors import CORSConfiguration
from sys import exit, argv
from optparse import OptionParser, make_option
from os import path
from pickle import dumps, loads

class FileStore(object):

   def __init__(self, proxy, processId, mode= 'r'):
      '''
      Gives r+w access to file on static instance via local tempfile.NamedTemproraryFile
      '''
      self.proxy= proxy
      self.processId= processId
      self.mode= mode

      self.tmpFile= NamedTemporaryFile(mode= 'r+', prefix= self.processId, delete= True)
      if 'r' in mode:
         self.__get__()

   def __get__(self):
      '''
      Get contents of static instance file and save to local temp file
      '''
      data= self.proxy.getFileContents(self.processId)
      self.tmpFile.write(data.tostring())
      self.tmpFile.seek(0)

   def __post__(self):
      '''
      Posts contents of local temp file to static instance file
      '''
      self.tmpFile.seek(0)
      data= self.tmpFile.read()
      self.proxy.setFileContents(self.processId, data)
      self.tmpFile.seek(0)

   def getName(self):
      return self.processId

   def getLocalName(self):
      return self.tmpFile.name

   def write(self, data):
      '''
      Writes data to local tempfile
      '''
      if 'w' not in self.mode:
         raise Exception('file open for read only')

      self.tmpFile.write(dumps(data))

   def read(self, size= -1):
      '''
      Reads data from local tempfile.  See file read() for more details.
      '''
      if 'r' not in self.mode:
         raise Exception('file open for write only')
  
      return loads(self.tmpFile.read(size))

   def readlines(self):
      '''
      Reads lines from local tempfile.  See file readlines() for more detals.
      '''
      if 'r' not in self.mode:
         raise Exception('file open for write only')

      return loads(self.tmpFile.readlines())

   def readline(self):
      '''
      Reads line from local tempfile. See file readline() for more details.
      '''
      if 'r' not in self.mode:
         raise Exception('file open for write only')

      return loads(self.tmpFile.readline())
 
   def close(self, delete= False):
      '''
      Saves the contents of the local tempfile and then closes/destroys the local tempfile.  See self.__post__() and python tempfile for more details.
      '''

      if 'w' in self.mode:
         self.__post__()

      elif 'r' in self.mode:

         # if delete requested -- remove file form static instance
         if delete:
            self.proxy.deleteFile(self.processId)

      self.tmpFile.close()
  
      

class S3File(object):

   def __init__(self, accessKey, secretKey, bucket, processId, mode= 'r'):
      '''
      Gives r+w access to s3File via local tempfile.NamedTemporaryFile
      '''
      self.processId= processId
      self.accessKey= accessKey
      self.secretKey= secretKey
      self.bucket= bucket
      self.mode= mode
      self.tmpFile= NamedTemporaryFile(mode= 'r+', prefix= self.processId, delete= True)

      #self.__s3connect__()

      if 'r' in mode:
         self.__get__()

   def __s3connect__(self):

      try:
         self.s3= S3Connection(self.accessKey, self.secretKey)
      except Exception, e:
         raise Exception('could not connect to s3 transport %s' % (str(e)))

      #self.s3cors= CORSConfiguration()
      #self.s3cors.add_rule(['GET', 'PUT', 'POST', 'DELETE'], '*')

   def __get__(self):
      '''
      Gets contents of s3 file and save to local temp file
      '''
      self.__s3connect__()
 
      # get key from bucket from s3
      bucket= self.s3.get_bucket(self.bucket)
      #bucket.set_cors(self.s3cors)
      key= bucket.get_key(self.processId)

      # get contents of s3 and save to tempfile
      key.get_contents_to_filename(self.tmpFile.name)

   def __post__(self):
      '''
      Gets contents of local temp file and saves to s3 file
      '''

      self.__s3connect__()

      # get name of tempfile and seek to the start
      self.tmpFile.seek(0)

      # create key in bucket on s3
      bucket= self.s3.create_bucket(self.bucket)
      #bucket.set_cors(self.s3cors)

      key= bucket.new_key(self.processId)
      key.set_metadata('processId', self.processId)
      key.set_metadata('datestamp', str(datetime.now()))

      # post the contents of the tempfile to s3
      key.set_contents_from_filename(self.tmpFile.name)

   def getName(self):
      '''
      Name of s3 file
      '''
      return self.processId

   def getLocalName(self):
      '''
      Name of local tempfile
      '''
      return self.tmpFile.name


   def write(self, data):
      '''
      Writes data to local tempfile
      '''
      if 'w' not in self.mode:
         raise Exception('file open for read only')

      self.tmpFile.write(dumps(data))

   def read(self, size= -1):
      '''
      Reads data from local tempfile.  See file read() for more details.
      '''
      if 'r' not in self.mode:
         raise Exception('file open for write only')
  
      return loads(self.tmpFile.read(size))

   def readlines(self):
      '''
      Reads lines from local tempfile.  See file readlines() for more detals.
      '''
      if 'r' not in self.mode:
         raise Exception('file open for write only')

      return loads(self.tmpFile.readlines())

   def readline(self):
      '''
      Reads line from local tempfile. See file readline() for more details.
      '''
      if 'r' not in self.mode:
         raise Exception('file open for write only')

      return loads(self.tmpFile.readline())
   
   def close(self, delete= False):
      '''
      Saves the contents of the local tempfile and then closes/destroys the local tempfile.  See self.__post__() and python tempfile for more details.
      '''

      if 'w' in self.mode:
         self.__post__()     

      elif 'r' in self.mode:

         # if delete requested -- remove file form s3
         if delete:
            bucket= self.s3.get_bucket(self.bucket)
            key= bucket.delete_key(self.processId)

      self.tmpFile.close()
