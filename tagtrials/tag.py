import schedule
import time
import configparser
import requests
import json
import sys
import traceback
from collections import Counter
from datetime import datetime
from itertools import chain

from pymongo import MongoClient
from bson.objectid import ObjectId

CONFIG_SECTION = 'App'
STAT_COLLECTION = 'stat'
TOP_NUMBER = 200
TRIAL_ELEMENTS = ['briefTitle', 'studyDesign', 'briefSummary', 'officialTitle', 'detailedDescription']

TOTAL_IDENTIFIERS = 'total_tags'


def main():
    config = read_config()
    db = get_db(config)
    t_c_name = config.get(CONFIG_SECTION, 'mongodb_trialcollection')
    trial_collection = db[t_c_name]
    stat_collection = db[STAT_COLLECTION]
    tag_service_url = config.get(CONFIG_SECTION, 'tag_service')
    print (f'.. tag service url: {tag_service_url}')
    tag_interval = config.getint(CONFIG_SECTION, 'tag_interval')
    if tag_interval <= 0 :
        tag_interval = 5
    
    # Task scheduling 
    # After every tag_interval/5 mins, do_tag will be called.  
    schedule.every(tag_interval).minutes.do(do_tag, trial_collection = trial_collection, \
        stat_collection = stat_collection, tag_service_url = tag_service_url)    
    
    while True:
        schedule.run_pending()
        time.sleep(1)

def tag_element(doc, doc_elem, url, name, keys):
    text = doc['untagged'][doc_elem]
    r = requests.post(url, json= {'doc':text, 'dict' : name})
    jr = json.loads(r.text)
    match = jr['match']
    for m in match:
        for en in m[2]:
            prim = en[1]
            if prim not in keys:
                keys.append(prim)
    return match

def tag_doc_dict(doc, tservice, ts_url):
    if not doc.get('dictionaries', None):
        doc['dictionaries'] = []
    name = tservice['name']
    found = None
    exist = [x for x in doc['dictionaries'] if x['name'] == name]
    if len(exist)>0:
        found = exist[0]
        if found['blacklist_timestamp'] >= tservice['blacklist_timestamp'] and found['whitelist_timestamp'] >= tservice['whitelist_timestamp']:
            return False # there is no need to retag.
    raw = {}
    keys = []
    raw['studyDesign'] = tag_element(doc, 'studyDesign', ts_url, name, keys)
    raw['briefSummary'] = tag_element(doc, 'briefSummary', ts_url, name, keys)
    raw['briefTitle'] = tag_element(doc, 'briefTitle', ts_url, name, keys)
    raw['officialTitle'] = tag_element(doc, 'officialTitle', ts_url, name, keys)
    raw['detailedDescription'] = tag_element(doc, 'detailedDescription', ts_url, name, keys)
    #raw['publications'] = tag_element(doc, 'publications', ts_url, name, keys)
    if found:
        found['raw'] = raw
        found['name'] = name
        found['blacklist_timestamp'] = tservice['blacklist_timestamp']
        found['whitelist_timestamp'] = tservice['whitelist_timestamp']
    else:
        doc['dictionaries'].append( {'raw': raw, 'name': name , \
            'blacklist_timestamp': tservice['blacklist_timestamp'], \
            'whitelist_timestamp' : tservice['whitelist_timestamp'] } )
    if name == 'protein':
        doc['primary_accession'] = keys
    elif name == 'pdb':
        doc['pdb_key'] = keys
    elif name == 'chembl':
        doc['chembl_key'] = keys
    elif name == 'pubchem':
        doc['pubchem_cid'] = keys
    return True

def create_key_link(name, keys):
    links = []
    if name == 'protein':
        for k in keys:
            url =  '<a href=\"https://aquaria.ws/' +  k + '\">Aquaria</a>' 
            links.append(url)
    elif name == 'pdb':
        for k in keys:
            url =  '<a href=\"https://www.rcsb.org/ligand/' +  k + '\">PDB</a>' 
            links.append(url)
    elif name == 'chembl':
        for k in keys:
            url =  '<a href=\"https://www.ebi.ac.uk/chembl/compound_report_card/' +  k + '/\">ChEMBL</a>' 
            links.append(url)
    elif name == 'pubchem':
        for k in keys:
            url =  '<a href=\"https://pubchem.ncbi.nlm.nih.gov/compound/' +  k + '\">PubChem</a>' 
            links.append(url)
    return links 

def create_links(links, name, raw):
    for m in raw:
        index = m[1]
        keys = []
        for en in m[2]:
            keys.append(en[1])
        k_links = create_key_link(name, keys)
        if index not in links.keys():
            links[index] = []
        links[index].extend(k_links) 

def tagged_item(doc, elem):
    original = doc['untagged'][elem]
    dict_raws = doc.get('dictionaries', None)
    links = {}
    if dict_raws:
        for dr in dict_raws:
            name = dr['name']
            raw = dr['raw'][elem]
            create_links(links, name, raw)
    lastTaken = -1 
    new_text = ''
    if len(links.keys()) == 0:
        return original 
    for index in sorted (links) :
        urls = links[index]
        new_text = new_text + original[lastTaken+1:index+1]
        lastTaken = index
        new_text = new_text + ' ('
        cursor = 0
        for url in urls:
            if cursor == 0:
                new_text = new_text + url
            else:
                new_text = new_text + ',' + url
            cursor = cursor + 1
        new_text = new_text + ')'
    if lastTaken < len(original) - 1:
        new_text = new_text + original[lastTaken+1:]
    return new_text

def generate_tagged(doc):
    tagged = {}
    tagged['studyDesign'] = tagged_item(doc, 'studyDesign')
    tagged['briefSummary'] = tagged_item(doc, 'briefSummary')
    tagged['briefTitle'] = tagged_item(doc, 'briefTitle')
    tagged['officialTitle'] = tagged_item(doc, 'officialTitle')
    tagged['detailedDescription'] = tagged_item(doc, 'detailedDescription')
    #tagged['publications'] = tagged_item(doc, 'publications')
    doc['tagged'] = tagged    
    
def tag_doc(collection, tid, ts_services, ts_url):
    doc = collection.find_one({'ctid': tid})
    doc_updated = False
    tagged_dicts = doc.get('dictionaries', None)
    for tservice in ts_services:
        updated = tag_doc_dict(doc, tservice, ts_url)
        if updated:
            doc_updated = True
    if doc_updated:
        doc['timestamp'] = datetime.now()
        generate_tagged(doc)
        collection.update_one({'_id':doc['_id']}, {"$set": doc}, upsert=False)
    return doc_updated

def tag(collection, tag_service_url):
    print (f'start tagging new trial or with new dictionary.')
    ts_services = get_all_tagservices(tag_service_url)
    ts_url = tag_service_url + 'tag'
    tag_updated = False
    for tid in get_all_trial_id(collection):
        if tag_doc(collection, tid, ts_services, ts_url):
            tag_updated = True
    return tag_updated

def get_all_tagservices(tag_service_url):
    url = tag_service_url + 'dictionaries/update'
    r = requests.post(url, json={})
    jdicts = json.loads(r.text)['dictionaries']
    for item in jdicts:
        strd = item['blacklist']
        if strd and len(strd) >0:
            item['blacklist_timestamp'] = datetime.fromisoformat(strd)
        else: # dummy timestamp
            item['blacklist_timestamp'] = datetime(2020, 1, 1)
        strd = item['whitelist']
        if strd and len(strd) >0:
            item['whitelist_timestamp'] = datetime.fromisoformat(strd)
        else: # dummy timestamp
            item['whitelist_timestamp'] = datetime(2020, 1, 1)
    return jdicts

def get_all_trial_id(collection):
    ctids = []
    for doc in collection.find(filter=None, projection={'ctid': True}):
        ctids.append(doc['ctid'])
    return ctids

def do_tag(trial_collection, stat_collection, tag_service_url):
    try:
        tag_updated = tag(trial_collection, tag_service_url)
    except:
        traceback.print_exc(file=sys.stdout)
    try:
        if tag_updated:
            update_statistics(trial_collection, stat_collection)
        else:
            print (f'There is no update of tags.')
    except:
        traceback.print_exc(file=sys.stdout)

def read_raw_tags(rawtags, text):
    tags = []
    if rawtags:
        for m in rawtags:
            word = text[m[0]:m[1]+1]
            identifiers = []
            for en in m[2]:
                identifiers.append(en[1])
            tags.append( {'word': word, 'identifiers' : identifiers} )
    return tags

def read_doc_tags(tid, trial_collection):
    doc = trial_collection.find_one({'ctid': tid})
    dicts = doc.get('dictionaries', None)
    untagged = doc.get('untagged', None)
    taginfo = []
    total_words = 0
    if dicts:
        for d_entry in dicts:
            t_info = { 'name': d_entry['name'], 'tags': [] }
            raw = d_entry['raw']
            for elem in TRIAL_ELEMENTS:
                text = untagged[elem]
                tags = read_raw_tags(raw[elem], text)
                t_info['tags'].extend(tags)
                total_words += len(text.split())
            taginfo.append(t_info)
    return taginfo, total_words

def generate_stats(trial_tags, stat_collection, total_words):
    doc_count = len(trial_tags)
    # grouping
    stats = []
    for doc_tags in trial_tags:
        for t_dict in doc_tags:
            name = t_dict['name']
            search = [x for x in stats if x['name'] == name]
            stats_dict = None
            if len(search) == 0:
                stats_dict = {'name' : name , 'words':[], 'identifiers': [], 'mentioned' : []}
                stats.append(stats_dict)
            else :
                stats_dict = search[0]
            for elem in t_dict['tags']:
                    stats_dict['words'].append( elem['word'] )
                    stats_dict['identifiers'].extend(elem['identifiers'])
            mentioned = set (chain.from_iterable(w['identifiers'] for w in t_dict['tags']))
            stats_dict['mentioned'].extend( mentioned )
    
    # start aggregating
    for stats_dict in stats:
        # words stat
        stat = {'total_trials': doc_count, 'total_words' : total_words}
        stat['total_tagged_words'] = len(stats_dict['words'])
        dist_words = set({v.casefold(): v for v in stats_dict['words']}.values()) 
        stat['distinct_tagged_words_case_insensitive'] = len(dist_words)
        if doc_count >0:
            stat['average_tagged_words_per_trial'] = float(len(stats_dict['words'])) / doc_count
            stat['average_distinct_tagged_words_per_trial'] = float(len(dist_words)) / doc_count
        stat['top200_tagged_words'] =  Counter(stats_dict['words']).most_common(TOP_NUMBER) 
        # identifier stat
        stat[TOTAL_IDENTIFIERS] = len(stats_dict['identifiers'])
        dist_identifiers = set (stats_dict['identifiers'])
        stat['distinct_identifiers'] = len(dist_identifiers)
        if doc_count >0:
            stat['average_identifiers_per_trial'] = float(len(stats_dict['identifiers'])) / doc_count
            stat['average_distinct_identifiers_per_trial'] = float(len(dist_identifiers)) / doc_count
        top200_identifiers = Counter(stats_dict['identifiers']).most_common(TOP_NUMBER)
        identifiers_mentioned = list(Counter(stats_dict['mentioned']).items())
        top_ids = []
        for item in top200_identifiers:
            identifier = item[0]
            search = [x for x in identifiers_mentioned if x[0] == identifier]
            top_ids.append( { identifier : {'tags': item[1], 'trials': search[0][1]} })
        stat['top200_identifiers'] =  top_ids
        save_statistics(stat, stats_dict['name'], stat_collection)

def save_statistics(data, name, stat_collection) :
    doc = {'name':name, 'data': data, 'timestamp': datetime.now()}
    stat_collection.update_one({'name':name}, {"$set": doc }, upsert=True)

def update_statistics(trial_collection, stat_collection):
    print (f'start updating statistics...')
    trial_tags = []
    total_words = 0
    for tid in get_all_trial_id(trial_collection):
        tags, word_count = read_doc_tags(tid, trial_collection)
        trial_tags.append( tags )
        total_words += word_count
    generate_stats(trial_tags, stat_collection, total_words)
    print (f'end updating statistics.')

def get_db(config):
    uri = config.get(CONFIG_SECTION, 'mongodb_uri')
    print (f'.. mongodb uri: {uri}')
    c = MongoClient( uri )
    dbname = config.get(CONFIG_SECTION, 'mongodb_db')
    print (f'\tdatabase: {dbname}')
    return c[dbname]

def read_config():
    config = configparser.ConfigParser()
    config.read('tag.cfg')
    return config

if __name__ == "__main__":
    main()