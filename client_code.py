import rpyc
import os
import sys
master = rpyc.connect('localhost',port = 18812, config={'allow_public_attrs': True})
master.root.init()
adr = 0
while(adr != -1):
	print("\nWelcome to our Project \n Choose your operation :\n(1)mkdir\n(2)rmdir\n(3)Display directory structure\n(4)Exit")
	adr = input()
	if adr == '1':
		f = input("\nEnter file name:")
		oper = master.root.mkdir(f)
		print(oper)
	
	elif adr == '2':
		f = input("\nEnter the name of dir to be deleted:")
		oper = master.root.rmdir(f)
		print(oper)

	elif adr=='3':
		oper=master.root.display()
		for i in oper:
			print(i,oper[i])
	elif adr == '4' :
		break
