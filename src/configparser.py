#!/usr/bin/env python

from sys import exit
from xml.etree import ElementTree as Parser

class ConfigParser(object):

   def loadConfig(self, taskSource, configFile):
      '''
      Read the configuration file for this category
      '''

      tree= Parser.parse(configFile)
      root= tree.getroot()

      source= self.loadSource(taskSource, root)
      self.loadVariables(source)

   def loadSource(self, taskSource, root):
      '''
      Load the data sources for this category
      '''

      sources= root.findall('source')
      for source in sources:
         if source.attrib.get('name') == taskSource:
            return source
      return None

   def loadVariables(self, source):
      '''
      Load the variables needed for this category
      '''

      variables= source.findall('variable')
      for variable in variables:

        try:
           type= eval(variable.attrib.get('type'))
        except:
           raise

        name= variable.attrib.get('name')
        val= variable.text

        if type in (str, int, float):
           setattr(self, name, type(val))

        elif type == dict:
           var= type()
           for item in variable.findall('item'):
              var.update([(item.attrib.get('name'), item.text)])
           setattr(self, name, var)

        elif type == list:
           var= type()
           for item in variable.findall('item'):
              var.append(item.text)
           setattr(self, name, var)
