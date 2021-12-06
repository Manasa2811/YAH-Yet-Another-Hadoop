import sys
import os
import rpyc

from rpyc.utils.server import ThreadedServer

class DatanodeServer(rpyc.Service):
	def exposed_Datanode():		
		if __name__ == "__main__":
			connection = rpyc.connect('localhost', port = 18812, config={"allow_all_attrs": True})
			th = ThreadedServer(DatanodeServer, port = 8888)
			th.start()
