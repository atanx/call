#!/usr/bin/python
# coding:utf-8

import os
import sys
import ConfigParser
import traceback
import exceptions
import collections


class Configuration:

	def __init__(self):
		default_home = os.path.join(sys.path[0], '..')
		self.__Home = os.getenv("DJHome", default_home)
		self.__Conf = collections.defaultdict(ConfigParser.ConfigParser)

		self.Structure_init()

		for type in self.__Conf_Structure :
			full_file = os.path.join(self.__Home, 'conf', self.__Conf_File[type])
			print full_file
			self.__Conf[type].read(full_file)

	def Structure_init(self):
		self.__Conf_Structure = {'core':{'default':[]},
								   'db':{'default':[]}
								}
		self.__Conf_File = { 'db':'db.conf',
						   'core':'core.conf'
						   }

	def get_Conf_Value(self,type,section='null',option='null'):
		try:
			if section == 'null' :
				result = collections.defaultdict(lambda :{})
				section_list = self.__Conf[type].sections()
				for tmp_section in section_list:
					option_list = self.__Conf[type].options(tmp_section)
					for tmp_option in option_list:
						result[tmp_section][tmp_option] = self.__Conf[type].get(tmp_section,tmp_option).strip()

			elif option == 'null':
				temp = self.__Conf[type].items(section)
				result = {}
				for key,value in temp:
					result[key] = value.strip()

			else:
				result = self.__Conf[type].get(section,option).strip()

		except:
			traceback.print_exc()
			#exit(1) #yeqs_flag
			if option == 'null' or section == 'null':
				result = {}
			else:
				result = ''
		return result



if __name__ == "__main__" :
	conf=Configuration()

