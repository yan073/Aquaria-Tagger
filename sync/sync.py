import schedule
import time
import configparser
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

CONFIG_SECTION = 'App'

def sync(src, dest, col):
    dt_string = datetime.now().strftime("%d/%m %H:%M")
    print (f'{dt_string} start syncing {col}...')
    src_coll = src[col]
    dest_coll = dest[col]
    doclist = src_coll.find(filter=None, projection={'_id': True, 'timestamp': True})
    for doc in doclist:
        tsrc = doc.get('timestamp', None)
        if tsrc is not None:
            foid = {'_id':ObjectId(doc.get('_id'))}
            target = dest_coll.find_one(foid)
            if target:
                if tsrc != target.get('timestamp', None) :
                    dest_coll.update(foid, {"$set": src_coll.find_one(foid) }, upsert=False)
                    docid = doc.get('_id')
                    print (f'Doc {docid} updated.')
            else:
                dest_coll.insert_one(src_coll.find_one(foid))
                docid = doc.get('_id')
                print (f'Doc {docid} inserted.')

def do_sync(db1, db2, s1to2, s2to1):
    for col in s1to2:
        if len(col) >0:
            sync(db1, db2, col)
    for col in s2to1:
        if len(col) >0:
            sync(db2, db1, col)

def main():
    config = read_config()
    db1 = get_db1(config)
    db2 = get_db2(config)
    s1to2 = config.get(CONFIG_SECTION, '1to2_collection').split()
    print (f'sync collections from db1 to db2: {s1to2}')
    s2to1 = config.get(CONFIG_SECTION, '2to1_collection').split()
    print (f'sync collections from db2 to db1: {s2to1}')

    # Task scheduling 
    # After every 5 mins, do_sync will be called.  
    schedule.every(5).minutes.do(do_sync, db1 = db1, db2 = db2, s1to2=s1to2, s2to1=s2to1)    
    
    while True:
        schedule.run_pending()
        time.sleep(1)

def get_db1(config):
    uri = config.get(CONFIG_SECTION, 'mongodb1_uri')
    c = MongoClient( uri )
    db = c[config.get(CONFIG_SECTION, 'mongodb1_db')]
    print (db)
    return db

def get_db2(config):
    uri = config.get(CONFIG_SECTION, 'mongodb2_uri')
    c = MongoClient( uri )
    db = c[config.get(CONFIG_SECTION, 'mongodb2_db')]
    print (db)
    return db

def read_config():
    config = configparser.ConfigParser()
    config.read('app.cfg')
    return config

if __name__ == "__main__":
    main()
