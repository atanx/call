#!/usr/bin/python
# coding:utf-8

import os,sys
import configuration
import datetime
import traceback
import subprocess
import commands
import re

print sys.path[0]

class ImportDB():
	
	def __init__(self):
		self.__Conf = configuration.Configuration()
		self.__db_conf = self.__Conf.get_Conf_Value('db')
		self.__Keyword = self.__Conf.get_Conf_Value('core','Hive_Keyword','keyword').strip().split(',')
		self.__Typedict = self.__Conf.get_Conf_Value('core','MysqlToHive_Type')
		self.__db = ""
		self.__dt = ""  
		self.__dt_zkb = ""  
		self.__tb = []
		self.__tb_pref = []
		self.__tb_fields = dict()
		self.__structure={}

	def read_options(self):
		today=datetime.date.today() 
		oneday=datetime.timedelta(days=1) 
		data_date=(today-oneday).strftime('%Y%m%d')
		self.__dt = data_date

		if len(sys.argv) <= 2:
			self.__db = 'call_db'
		elif len(sys.argv) == 3:
			self.__db = sys.argv[1]
			self.__dt = sys.argv[2]
		else:
			self.__db = sys.argv[1]
			self.__dt = sys.argv[2]
			self.__tb = sys.argv[3].split(",")

		db_conf = self.__Conf.get_Conf_Value('db')

		if self.__db not in db_conf:
			print "db不合法"
			sys.exit(1)

		if len(self.__tb) == 0:
			self.__tb = [table.strip() for table in db_conf[self.__db]["db_list"].split(",") if table]
			self.__tb_pref = [table.strip() for table in db_conf[self.__db]["tb_pref_list"].split(',') ]
	
	def print_test(self):
		print self.__db,self.__dt,self.__tb,self.__structure

	def parse_table_fields(self, mysql_table_name):
		lines = commands.getoutput("sed -n '/CREATE TABLE `%s`/,/) ENGINE/p' %s.sql" % (mysql_table_name,self.__db)).split('\n')[1:-1]
		fields = []
		for line in lines:
			line = line.lstrip()
			if line.startswith('KEY') or line.startswith('PRIMARY KEY') or line.startswith('UNIQUE KEY'):
				continue
			field, field_type = line.split(' ',2)[:2]
			field = field.replace('`','')
			if field in self.__Keyword:
				field = 'trans_' + field
			field_type = field_type.split('(')[0]
			field_type = field_type.strip(',')
			fields.append((field, field_type))
		return fields

	def table_grouping(self):
		# 收集call_db中所有表名开头为"crm_customerinfo"或"crm_custominfo_field"等为前缀的表。
		# 这些前缀配置在conf/db.conf中，读取在self.__tb_pref里， self.__tb_pref是一个list列表。
		context = commands.getoutput("sed -n '/CREATE TABLE/p' %s.sql" % self.__db)
		groups = dict(zip(self.__tb_pref, [[]] * len(self.__tb_pref)))
		for line in context.split('\n')[1:-1] :
			line = line.lstrip()
			parts = line.split('`')
			if parts<2:
				continue
			table = parts[1]
			for pref in self.__tb_pref:
				if table.startswith(pref):
					groups[pref] = groups[pref] + [table]
					break
		return groups
		
	@staticmethod
	def index_by_value(_list, values):
		index = []
		for val in values:
			idx = None
			try:
				idx = _list.index(val)
			except:
				pass
			index.append(idx)
		return index
	
	@staticmethod
	def value_by_index(_list, index):
		values = []
		for idx in index:
			val = '\N'
			try:
				val = _list[idx]
			except:
				pass
			values.append(val)
		return values
	
	def load_mysql_schema(self):
		hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + self.__dt + "/db_xcall.sql.bz2"
		subprocess.call("hadoop fs -text %s | sed -n '/CREATE TABLE/,/) ENGINE/p' > %s.sql" % (hdfs_path,self.__db), shell=True)
		groups = self.table_grouping()
		self.__tb_groups = groups
		
		# 有分表时，优先使用配置文件中的字段
		for group_name in self.__tb_groups:
			hive_fields = self.__Conf.get_Conf_Value('db',self.__db, group_name).split(',')
			if any(hive_fields):
				self.__structure[group_name] = hive_fields
			else:
				self.__structure[group_name] = []
			#	self.__structure[group_name] = self.parse_table_fields(self.__tb_groups[group_name][0])
			print group_name, hive_fields
			for table in groups[group_name]:
				mysql_fields = [f[0] for f in self.parse_table_fields(table)]
				mysql_fields_idx = self.index_by_value(mysql_fields, hive_fields)
				self.__tb_fields[table] = mysql_fields_idx
					
		for table in self.__tb:
			self.__structure[table] = self.parse_table_fields(table)
		
		
		#subprocess.call("rm -f %s.sql" % self.__db, shell=True)

	def create_hive_table(self):
		hive_tables = self.__tb + self.__tb_pref
		for table in hive_tables:
			fields = self.__structure[table]
			if  not any(fields):
				continue
			hive_sql = 'use ' + self.__db + '; '
			hive_sql += 'drop table if exists %s ; ' % (table)
			hive_sql += 'create table %s ( ' % (table)
			for i in range(len(fields)) :
				try:
					field, field_type = fields[i]
				except:
					field = fields[i]
					field_type = 'varchar'
				field_type = self.__Typedict[field_type]
				hive_sql += '%s %s,' % (field, field_type)
			hive_sql = hive_sql.strip(',')
			hive_sql += ') '
			hive_sql += ' row format delimited fields terminated by \'\\t\' '
			hive_cmd = 'hive -S -e "%s"' % hive_sql
			print hive_cmd
			subprocess.call(hive_cmd, shell=True)
		

	@staticmethod
	def split_row(line, sep):
		parts = []
		status = 0
		part = ''
		for index in range(0, len(line)):
			ch = line[index]
			if ch == "'":
				i = index
				while i >= 0:
					i -= 1
					if i>=0 and line[i] != '\\':
						break
				if (index - i) % 2 == 1:
					status = 1 - status
			if status == 0 and ch == sep:
				parts.append(part)
				part = ''
			else:
				part += ch
		parts.append(part)
		return parts

	@staticmethod
	def replace(row, old, new):
		for (i, v) in enumerate(row):
			if v == old:
				row[i] = new
			if v[0] == "'" and v[-1] == "'":
				row[i] = v[1:-1]

	def to_hive(self):
		output = open('%s_%s.db.info' % (self.__db,self.__dt), 'w+')
		hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + self.__dt + "/db_xcall.sql.bz2"
		response = os.popen("hadoop fs -text %s " % (hdfs_path)).readlines()
		for line in response:
			parts = line.split('`',2)
			if line.startswith("INSERT") and len(parts)==3:
				mysql_table = parts[1]
				hive_table = mysql_table
				need_skip = True
				if mysql_table in self.__tb:
					need_skip = False
				for group_name, tables in self.__tb_groups.iteritems():
					if mysql_table in tables and any(self.__structure[group_name]):
						hive_table = group_name
						need_skip = False
						break
				if need_skip:
					continue
				for item in self.split_row(line[line.find('(')+1:-1], '('):
					row = self.split_row(item[:-2], ',')
					#print "~"*20
					#print row
					# 只保留需要的字段
					fields_idx = self.__tb_fields.get(mysql_table,[])
					if fields_idx:
						row = self.value_by_index(row, fields_idx)
					#print mysql_table
					#print fields_idx
					#print row
					self.replace(row, 'NULL', '\N')
					tag = self.__db+'#'+self.__dt+'#'+hive_table+'#'
					output.write(tag + "\t" + '\t'.join(row) + "\n")
					#print tag + "\t" + '\t'.join(row)
		output.close()
		
		# 生成hive表数据文件
		selected_tables = self.__tb + self.__tb_groups.keys()
		for table in selected_tables:
			db_file = "%s_%s.db.info" % (self.__db,self.__dt)
			table_file = "%s_%s_%s.tb.info" % (self.__db,self.__dt,table)
			table_tag = '%s#%s#%s#' % (self.__db,self.__dt,table)
			os.system(" grep '%s' %s|sed -e 's/%s\t//g' > %s " % (table_tag,db_file,table_tag,table_file)  )

			subprocess.call(\
						'hive -S -e "load data local inpath \'%s\' overwrite into table %s.%s;"' \
						% (table_file,self.__db,table), shell=True) 
		#	os.system("rm -f %s" % table_file)

		#os.system("rm -f %s_%s.db.info" % (self.__db,self.__dt))


if __name__ == '__main__':
	job = ImportDB()
	job.read_options()
	job.load_mysql_schema()
	job.create_hive_table()
	job.to_hive()
