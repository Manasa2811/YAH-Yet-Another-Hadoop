# This file allows user to create new configurations for dfs and use the ones they already created

import sys
import json
import glob
import os
import namenode

def getLastDFSConfFile():
    filess = glob.glob('dfs_setup_config*.json')
    maxim = 0

    for i in range(len(filess)):
        maxim = max(maxim, int(filess[i].split('dfs_setup_config')[1].split('.json')[0]))

    return maxim

def display(path):
    f = json.load(open(path))
    key =  list(f.keys())
    
    print('Here is your configuration:')
    
    for k in key:
        print(k + ':' + str(f[k]))
    
    print()
    print('You can now start manipulating your dfs')

def createNewDFS():
    fileNo = getLastDFSConfFile()
    newDFSConfigFile = 'dfs_setup_config'+str(fileNo+1)+'.json'
    
    defaultConfig = json.load(open('config.json'))
    defaultConfig['path_to_datanodes'] = 'GlobalFS/'+str(fileNo+1)+'/DATANODE'
    defaultConfig['path_to_namenodes'] = 'GlobalFS/'+str(fileNo+1)+'/NAMENODE'
    defaultConfig['datanode_log_path'] = 'GlobalFS/'+str(fileNo+1)+'/DATANODE/LOGS'
    defaultConfig['namenode_log_path'] = 'GlobalFS/'+str(fileNo+1)+'/NAMENODE/log.json'
    defaultConfig['namenode_checkpoints'] = 'GlobalFS/'+str(fileNo+1)+'/NAMENODE/CHECKPOINTS'
    defaultConfig['dfs_setup_config'] = newDFSConfigFile
    
    with open(newDFSConfigFile, "w") as outfile:
        json.dump(defaultConfig, outfile)
    
    # create the required folders and files
    for attr in defaultConfig:
        if ('path' in attr or 'checkpoints' in attr) and attr != 'namenode_log_path' and attr != 'fs_path':
            os.makedirs(defaultConfig[attr])   
    
    open(defaultConfig['namenode_log_path'], 'w')
    
    for i in range(defaultConfig['num_datanodes']):
        open(defaultConfig['path_to_datanodes']+'/DNODE'+str(i+1), 'w')
    
    for i in range(defaultConfig['num_datanodes']):
        open(defaultConfig['datanode_log_path']+'/DNODE'+str(i+1)+'LOG.json', 'w')

    print('New DFS created, the config file is named dfs_setup_config'+str(fileNo+1)+'.json')
    print('Pass the above filename as command line argument the next time')
    
    print()
    print('Would you like to format your namenode? (y/n)')
    ans = input()
    if ans == 'y':
        
        # delete namenode
        f = open('GlobalFS/'+str(fileNo+1)+'/NAMENODE/log.json', 'w')
        f.truncate(0)
        f.close()

        # delete datanodes
        for i in range(defaultConfig['num_datanodes']):
            f = open(defaultConfig['path_to_datanodes']+'/DNODE'+str(i+1), 'w')
            f.truncate(0)
            f.close()

            f = open(defaultConfig['datanode_log_path']+'/DNODE'+str(i+1)+'LOG.json', 'w')
            f.truncate(0)
            f.close()    
        
        print('formatted successfully')
    else:
        print('not formatting namenode')

    return ['GlobalFS/'+str(fileNo+1), newDFSConfigFile]

def config_setup():
    
    if len(sys.argv) == 2:
        # verify if it exists
        try:
            f = open(sys.argv[1])
            folderName = int(sys.argv[1].split('dfs_setup_config')[1].split('.json')[0])
            pathToDfs = ['GlobalFS/'+str(folderName) ,sys.argv[1]]

        except:
            print('Config file does not exist. Creating new dfs with default settings')
            pathToDfs = createNewDFS()

    else:
        pathToDfs = createNewDFS()
    
    display(pathToDfs[1])
    #namenode.run(pathToDfs)

config_setup()
