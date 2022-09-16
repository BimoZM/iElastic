import os
import json
from posixpath import splitext
import requests
from elasticsearch import Elasticsearch
import sys
import uuid
from pathlib import Path



def print_help():
    print(f"""
    {sys.argv[0]} <json folder> [option]
    

    example:
    {sys.argv[0]} /tmp/jsonfile/ --silent
    {sys.argv[0]} /tmp/jsonfile/ --prefix "case01"
    """)
def scanRecurse(baseDir):
    '''
    Scan a directory and return a list of all files
    return: list of files
    '''
    for entry in os.scandir(baseDir):
        if entry.is_file():
            yield entry
        else:
            yield from scanRecurse(entry.path)
def readJson(target):
    with open(target, 'r') as f:
        data = bigjson.load(f)
        for index, item in enumerate(data):
            print(index,dict(item))


def ingestJson2Elastic(t, pre, host, port, v=True):
    """import json file to elasticsearch
    
    :params t: (path) path of the json file
    :params pre: (str) prefix index of the data
    :params host:
    :params port:
    :params v: (boolean) if true mean, verbose the loop
    :return: None
    """
    
    try:
        target = t
        prefix = pre
        filePath = Path(t)
        extension = filePath.suffix.lower()
        if extension == ".json":
            full_path = target
        else:
            print("target is not json file --> '{filePath}'")
            exit()

    except KeyboardInterrupt as e:
        print(e)

    try:
        print("[*] Connecting to database")
        re = requests.get(f"http://{host}:{port}")
        es = Elasticsearch(f"http://{host}:{port}")
        if re.status_code != 200:
            print(f"[!] Error with status code: {re.status_code}")
        else:
            print("\n")
            print("[*] Connected to database")
            print("[*] Waiting for importing data. . .")
            with open(full_path, 'r') as f:
                data = json.load(f)
            count = 0
            for index, item in enumerate(data):
                id = "-".join((prefix,str(uuid.uuid4())))
                es.index(
                    index="testing", 
                    id = id,
                    document=item)
                if v:
                    if count%1000 == 0 and count!= 0:
                       print(f'---> Index {count} has been imported well !') 
                count += 1
            
            print("\n")
            print(f"[*] Successfully Importing '{target}'! ! !")
    except (KeyError, ConnectionError) as error:
        print(f"{error}")


host = "localhost"
port = "9200"
#path = ("/home/avanza/Documents/json_files/")



if len(sys.argv) < 2:
    print_help()
    exit()


optionHandler = sys.argv[2:]
directory = sys.argv[1]
verbose = True



if '--prefix' in optionHandler:
    prefix = sys.argv[optionHandler.index('--prefix') + 1]
else:
    prefix = None

if '--silent' in optionHandler:
    verbose = False




# get all file names from the directory and save it to list
listDir = []
print("-"*30)
countFile = 0
for item in scanRecurse(directory):
    filePath = Path(item)
    print(filePath)
    countFile += 1
    listDir.append(filePath)
print("-"*30)
print(f"[*] {countFile} File detected")

for p in listDir:
    if prefix == None:
        prefix = str(p).split("\\")[-1][:5]
    
    
    ingestJson2Elastic(p, prefix, host, port, verbose)
    #readJson(p)