#importing necassary modules
import json
import sys
import argparse
import rpyc 
from rpyc.utils.server import ThreadedServer
import math
import os.path
import datetime
from collections import defaultdict



class NamenodeServer(rpyc.Service):
	fs_path = ""
	file_table = defaultdict(list)
	block_size = 0
	client = 1
	def on_connect(self, conn):
		dtdt = datetime.datetime.now()
		
		print(f"Client connected on {dtdt}")
	def on_disconnect(self, conn):
		dtdt = datetime.datetime.now()
		self.client -= 1
		print(f"Client Disconnected on {dtdt}")
		th.close()
	
	def exposed_init(self):
		self.file_table = defaultdict(list)		
		self.file_table[self.fs_path] = []
		print("init",self.file_table)
	
	def exposed_display(self):
		temp = self.file_table
		return(self.file_table)	
	
	def exposed_mkdir(self,dir):
		dire_name = dir.split("/")
		flag = 1
		#dir = self.fs_path + dir
		if len(dire_name) > 1:
			string = ''
			flag = 0
			for i in dire_name[:-1]:
				string = i + "/"
		if flag == 0:
			parent = self.fs_path + string
		elif flag == 1:
			parent = self.fs_path
		
		
		if(self.dir_exists(parent) == False):
			return "Parent directory does not exist"
		if(self.dir_exists(self.fs_path + dire_name[0] + "/") == True and len(dire_name) == 1):
			return "Directory already exist"
		if(dire_name[0] not in self.file_table[self.fs_path]):
			self.file_table[self.fs_path].append(dire_name[0])
			#print("entry into file table:",self.file_table)
			self.file_table[self.fs_path + dire_name[0] + "/"] = []
		if len(dire_name) > 1:
			for i in range(len(dire_name) - 1):
			
				tmp_s = ''
				for j in dire_name[:i+1]:
					tmp_s = j + "/"
					
				self.file_table[self.fs_path + tmp_s].append(dire_name[i+1])
				self.file_table[self.fs_path + tmp_s + dire_name[i+1] + "/"] = []
		
		print(self.file_table)
		return "\n	----Created new Directory----"
	
	def exposed_rmdir(self,dir):
		dir_name = dir.split("/")
		reme_dir = ''
		string = ''
		for i in dir_name[:-1]:
			string = i + "/"
		if(len(dir_name) > 1):
			reme_dir = self.fs_path + string + dir_name[-1] + "/"
		else:
			reme_dir = self.fs_path + dir_name[0] + "/"
		print(reme_dir)
		parent = self.fs_path + string
			
		if(self.dir_exists(parent) == False):
			return "\nParent directory does not exist"
		elif(dir_name[-1] not in self.file_table[parent]):
			return "\n Directory does not exist"
		if(len(self.file_table[reme_dir]) >= 1):
			return "Directory not empty"
		self.file_table[parent].remove(dir_name[-1])
		del self.file_table[reme_dir]
		print(self.file_table)
		return "\n	----Deleted Directory----"
	def dir_exists(self,file):
		if(file in self.file_table):
			return 1
			
		

if __name__=="__main__":
	
	parser = argparse.ArgumentParser() #parsing the config file to get values
	parser.add_argument('--config', required=False)
	args = parser.parse_args()
	subname = args.config
	if(subname):
		f = open(subname,)
	else:
		f = open("config.json",)

	dat = json.load(f)
	if(len(dat) !=12):
		print("incorrect config file")
		sys.exit()
	config = []
	for i in dat:
		config.append(dat[i])
	
	NamenodeServer.block_size = config[0]
	NamenodeServer.path_to_datanodes = config[1]
	NamenodeServer.path_to_namenodes = config[2]
	NamenodeServer.rep_f = config[3]
	NamenodeServer.num_d = config[4]
	NamenodeServer.datanode_size = config[5]
	NamenodeServer.sync_period = config[6]
	NamenodeServer.datanode_log_path = config[7]
	NamenodeServer.namenode_log_path = config[8]
	NamenodeServer.namenode_checkpoints = config[9]
	NamenodeServer.fs_path = config[10]
	NamenodeServer.dfs_setup_config = config[11]
	NamenodeServer.file_table = {}
	
	
	#starting namenode service on port 18812
	th = ThreadedServer(NamenodeServer, port=18812)
	th.start()
