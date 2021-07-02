from pymongo import MongoClient
import json
import requests
from datetime import datetime

dburi = "mongodb://localhost/"

dbname = "nihtrial"

c = MongoClient( dburi )
db = c[ dbname ]
trial_collection = db['trial']
protein_dict = db['entitydictionary'].find_one()['dictionary']
cathdb_url = 'http://www.cathdb.info/version/v4_3_0/api/rest/uniprot_to_funfam/'
cath_dict = {}
TRIAL_ELEMENTS = ['briefTitle', 'studyDesign', 'briefSummary', 'officialTitle', 'detailedDescription']

def get_protein_trial_map(trial_tags):
    ptmap = {}
    for trtg in trial_tags:
        for tt in trtg:
            if tt['name'] == 'protein':
                ctid = tt['ctid']
                for tag in tt['tags']:
                    for identifier in tag['identifiers']:
                        if identifier not in ptmap.keys():
                            ptmap[identifier] = []
                        if ctid not in ptmap[identifier]:
                            ptmap[identifier].append(ctid)
    return ptmap


doc = db['stat'].find_one({'name':'protein'})
words = doc['data']['top200_tagged_words']

def get_all_trial_id(collection):
    ctids = []
    for doc in collection.find(filter=None, projection={'ctid': True}):
        ctids.append(doc['ctid'])
    return ctids

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
            t_info = { 'name': d_entry['name'], 'tags': [], 'ctid' : tid }
            raw = d_entry['raw']
            for elem in TRIAL_ELEMENTS:
                text = untagged[elem]
                tags = read_raw_tags(raw[elem], text)
                t_info['tags'].extend(tags)
                total_words += len(text.split())
            taginfo.append(t_info)
    return taginfo, total_words

def get_preferred_name(key):
    exist = [x for x in protein_dict if x['primary_accession'] == key]
    if len(exist)>0:
        return exist[0]['words'][0]

def get_cath_for_primary_accession(pacc):
    if pacc not in cath_dict.keys():
        response =  requests.get( cathdb_url + pacc )
        if (response.status_code == 200):
            robj = response.json()
            pcath = [];    
            for rjd in robj['data']:
                sfid = rjd['superfamily_id']
                if sfid not in pcath:
                    pcath.append(sfid)
            cath_dict[pacc] = pcath
    return cath_dict[pacc]

def add_into_cath_p_map(cath_p_map, cath_ids, leaf):
    if len(cath_ids) == 0:
        cath_p_map['unknown'].append(leaf)
    else:
        for cid in cath_ids:
            if cid not in cath_p_map.keys():
                cath_p_map[cid] = [leaf]
            else:
                cath_p_map[cid].append(leaf)

def get_tooltip_trials(size, trials):
    info = '<p>Total number of clinical trials mentioning this protein: ' + str(size) + '</p>'
    info += '<p>List of trials mentioning this protein: '
    count = 0
    for ctid in trials:
        trul = '<a href="https://ClinicalTrials.gov/ct2/show/record/' + ctid + '"><strong>' + ctid + '</strong></a>'
        if count > 0:
            trul =', ' + trul
        info += trul
        count += 1
    info += '</p>'
    return info

def get_tooltip_aquaria(name):
    return '<a href="https://aquaria.app/' + name + '"><strong>' + name + '</strong></a>'

def get_cath_protein(leaf, cath, protein_trial_map):
    lobj = {'name' : leaf['name'], 'protein' : leaf['protein'], 'size' : leaf['size']}
    tooltip = get_tooltip_aquaria(leaf['name'])
    tooltip += ', <a href="http://www.cathdb.info/version/latest/superfamily/' + cath + '/classification" ><strong>' + cath +'</strong></a>'
    lobj['tooltip'] =  tooltip + get_tooltip_trials(leaf['size'], protein_trial_map[leaf['protein']])
    return lobj        

def get_nocath_protein(leaf, protein_trial_map):
    lobj = {'name' : leaf['name'], 'protein' : leaf['protein'], 'size' : leaf['size']}
    lobj['tooltip'] = get_tooltip_aquaria(leaf['name']) + get_tooltip_trials(leaf['size'], protein_trial_map[leaf['protein']])
    return lobj

def make_sure_cathobj_exist(v_data, l1, l2, cath):
    l1obj = None
    exist = [x for x in v_data["children"] if x['name'] == l1]
    if len(exist) > 0:
        l1obj = exist[0]
    else:
        l1obj = {"name": l1, "children": []}
        v_data["children"].append(l1obj)
    l2obj = None
    exist = [x for x in l1obj["children"] if x['name'] == l2]
    if len(exist) > 0:
        l2obj = exist[0]
    else:
        l2obj = {"name": l2, "children": []}
        l1obj["children"].append(l2obj)
    exist = [x for x in l2obj["children"] if x['name'] == cath]
    if len(exist) > 0:
        return exist[0]
    else:
        cathobj = {"name": cath, "children": []}
        l2obj["children"].append(cathobj)
        return cathobj
        
def dump_protein_visual_data(trial_tags, doc):
    protein_trial_map = get_protein_trial_map(trial_tags)
    cath_p_map = {'unknown' : []}
    identifiers = doc['data']['top200_identifiers']
    # get cath-> proteins map.
    for idf in identifiers:
        for key in idf:
            leaf = {'name': get_preferred_name(key), 'protein' : key}
            leaf['size'] = idf[key]['trials']
            cath_ids = get_cath_for_primary_accession( key )
            add_into_cath_p_map(cath_p_map, cath_ids, leaf)
    # start dumping data to v_data
    u_obj = {"name": 'unknown', "children": []}
    v_data = {"name": "statistics", "children": [u_obj]}
    for cath in cath_p_map.keys():
        if cath == 'unknown':
            for leaf in cath_p_map[cath]:
                lobj = get_nocath_protein(leaf, protein_trial_map)
                u_obj["children"].append( lobj )
        else:
            index = cath.find('.')
            l1 = cath[:index]
            index = cath.find('.', index + 1)
            l2 = cath[:index]
            cathobj = make_sure_cathobj_exist(v_data, l1, l2, cath)
            for leaf in cath_p_map[cath]:
                lobj = get_cath_protein(leaf, cath, protein_trial_map)
                cathobj['children'].append(lobj)
    with open('protein_cath.json', 'w') as outfile:
        json.dump(v_data, outfile)

def main():
    trial_tags = []
    total_words = 0
    for tid in get_all_trial_id(trial_collection):
        tags, word_count = read_doc_tags(tid, trial_collection)
        trial_tags.append( tags )
        total_words += word_count
    dump_protein_visual_data(trial_tags, db['stat'].find_one({'name':'protein'}))

if __name__ == "__main__":
    main()