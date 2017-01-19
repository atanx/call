import os
os.chdir('/app/bi/public_script/call/bin')
import configuration

conf = Configuration()
conf.get_Conf_Value('db')
conf.get_Conf_Value('db', 'call_db'):

import hdfs_to_hive
reload(hdfs_to_hive)
db = hdfs_to_hive.ImportDB()

db.read_options()
db.load_mysql_schema()
db.create_hive_table()
db.to_hive()


db._ImportDB__tb_groups
db._ImportDB__tb_pref
db._ImportDB__structure


db._ImportDB__tb_groups['crm_customerinfo_fields_conf'][0]
db._ImportDB__tb_groups['crm_customerinfo'].__len__()



