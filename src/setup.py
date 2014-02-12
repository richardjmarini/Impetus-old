
from distutils.core import setup, Extension

setup(
   name= 'Impetus',
   version= '1.0',
   description= 'Impetus Async Distributed Processing Framework',
   author= 'Richard J. Marini',
   author_email= 'richardjmarini@gmail.com',
   url= '',
   long_description= '''
      This package is a python high level interface for submitting tasks to the Impetus
      Autoscaling Asynchronous Distributed Processing Framework
   ''',
   py_modules= ['bot', 'task', 'transports', 'configparser']

)

