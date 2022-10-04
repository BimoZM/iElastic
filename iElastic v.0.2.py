from ast import arg
import os
from elasticsearch import Elasticsearch
import argparse
import json
import uuid
import requests
from pathlib import Path

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

def ingestJson2Elastic(*args):
    """import json file to elasticsearch
    
    :params t: (path) path of the json file
    :params idx: (str) doc_index index of the data
    :params host:
    :params port:
    :params v: (boolean) if true mean, verbose the loop
    :return: None
    """
    try:
        target = args[0]
        prefix = args[1]
        host = args[2]
        port = args[3]
        doc_type = args[4]
        doc_index = args[5]
        v = args[6]

        # maybe we could move this json file checking out of the function
        filePath = Path(target)
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
                item['doc_type'] = doc_type
                # Check for same UUID
                id_exists = True
                while id_exists:
                    if es.exists(index=doc_index, id=id):
                        id = "-".join((prefix,str(uuid.uuid4())))
                    else:
                        id_exists=False
                es.index(
                    index=doc_index,   # CHANGE THE ELASTICSEARCH INDEX HERE 
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

parser = argparse.ArgumentParser(description='Ingesting JSON file into the elasticsearch database')
requiredOne = parser.add_argument_group('Required one')
requiredOne.add_argument('-d', '--directory', type=str, metavar='', help='Path to directory')
requiredOne.add_argument('-f', '--file', type=str, metavar='', help='Path to file')
requireAll = parser.add_argument_group('Requried All')
requireAll.add_argument('-p','--prefix',type=str,metavar='',help='id prefix', required=True)
requireAll.add_argument('-D', '--doc', type=str, required=True, metavar='', help='Document type')
requireAll.add_argument('-i','--index',type=str,metavar='',help='Document index', required=True)
group = parser.add_mutually_exclusive_group()
group.add_argument('-v', '--verbose', action='store_true', help='print verbose')
group.add_argument('-q', '--quite', action='store_true', help='print quite')
args = parser.parse_args()

if __name__ == '__main__':
    # print(args.file, args.prefix, args.doc, args.index)
    # exit()
    host = "localhost"
    port = "9200"
    # ingestJson2Elastic(type, doc_index, host, port)
    listDir = []
    directory = args.file
    try:
        if args.directory:
            directory = args.directory
            # is directory exists?
            if not os.path.exists(directory):
                print(f"Directory '{directory}' is not found" )
                exit()
        elif args.file:
            # is the file exists?
            file = args.file
            if os.path.exists(file):
                # append path from sys.argv[2] to the listDir
                listDir = [file]
            else:
                print(f"file '{file}' not found")
                exit()
    except NotADirectoryError as e:
        print("[!] Option -d indicated not a directory")
        exit()
    
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

    prefix = args.prefix
    doc_type = args.doc
    doc_index = args.index
    countNonJsonFile = 0
    for p in listDir:
        # automatic define prefix to the 5 char of the first name file.

        if prefix == None:
            prefix = str(p).split("\\")[-1][:5]
        
        # check the extension
        filePath = Path(p)
        ex = filePath.suffix.lower()
        if ex == ".json":
            if args.verbose:
                v = True
                ingestJson2Elastic(p, prefix, host, port, doc_type, doc_index, v)
            elif args.qutie:
                v = False
                ingestJson2Elastic(p, prefix, host, port, doc_type, doc_index, v)
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