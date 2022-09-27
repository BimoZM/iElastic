from email.policy import strict
import os
import json
import requests
from elasticsearch import Elasticsearch
import sys
import uuid
from pathlib import Path



def print_help():
    print(f"""
    {sys.argv[0]}  [-d / -f] <json folder / json file> [option]
    example:
    {sys.argv[0]} -f /tmp/example.json    --> for single file
    {sys.argv[0]} /tmp/jsonfolder/     --> looking json file in the target folder recursively
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


# if problem with json, use bigjson
"""
def readJson(target):
    with open(target, 'r') as f:
        data = bigjson.load(f)
        for index, item in enumerate(data):
            print(index,dict(item))
"""




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

        # maybe we could move this json file checking out of the function
        filePath = Path(t)
        extension = filePath.suffix.lower()
        if extension == ".json":
            full_path = target
        else:
            print("target is not json file --> '{filePath}'")
            return None

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
            with open(full_path, 'r', encoding='utf-8') as f:
                data = json.load(f, strict=False)
            count = 0
            for index, item in enumerate(data):
                id = "-".join((prefix,str(uuid.uuid4())))
                es.index(
                    index="user_profiles",   # CHANGE THE ELASTICSEARCH INDEX HERE 
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

# Check for prefix option
if '--prefix' in optionHandler:
    prefix = sys.argv[optionHandler.index('--prefix') + 3]
else:
    prefix = None


if '--silent' in optionHandler:
    verbose = False

listDir = []
if '-d' == sys.argv[1]:
    directory = sys.argv[2]
# is directory exists?
    if not os.path.exists(directory):
        print(f"Directory '{directory}' is not found" )
        exit()
elif '-f' == sys.argv[1]:

    # is the file exists?
    if os.path.exists(sys.argv[2]):
        # append path from sys.argv[2] to the listDir
        listDir = [sys.argv[2]]
    else:
        print(f"file '{sys.argv[2]}' not found")
        exit()

# is directory exists?



# get all file names from the directory and save it to list
print("-"*30)
if len(listDir) != 1:
    for item in scanRecurse(directory):
        filePath = Path(item)
        print(filePath)
        listDir.append(filePath)
else:
    print(listDir[0])
print("-"*30)
print(f"[*] {len(listDir)} File detected")

if len(listDir) == 0:
    exit()


countNonJsonFile = 0
for p in listDir:

    # automatic define prefix to the 5 char of the first name file.
    if prefix == None:
        prefix = str(p).split("\\")[-1][:5]
    
    # check the extension
    filePath = Path(p)
    ex = filePath.suffix.lower()
    if ex == ".json":
        ingestJson2Elastic(p, prefix, host, port, verbose)
    else:

        # if the list file less that 10, show the file name of the non json file
        # else, just count the file.
        if len(listDir) < 10:
            print(f"target is not json file --> '{filePath}'")
        else:
            countNonJsonFile += 1
        continue
    
    #readJson(p)
# show the number of non json file
if countNonJsonFile:
    print(f"Found {countNonJsonFile} non json file, and not loaded")