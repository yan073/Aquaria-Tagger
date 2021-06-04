from pymongo import MongoClient
import json

dburi = "mongodb://localhost/"
dbname = "covidtag"
c = MongoClient( dburi )
db = c[ dbname ]
pubchem_cluster = db['chemical_cluster'].find_one({'name': 'pubchem_cluster'})['data']

def load_pubchem_mapping():
    mapping = {}
    for record in db['chemical_cluster'].find({'name': 'chembl2pubchem'}):
        sub = record['data']
        mapping = {**mapping, **sub}
    return mapping

def get_clusters(pubchem_id):
    if pubchem_id:
        exist = [x for x in pubchem_cluster if x['pubchem_id'] == pubchem_id]
        if len(exist)>0:
            return exist[0]['clusters']
    return None    

def get_tooltip(pubchem_id, compound, chembl, size):
    content = ''
    if len(pubchem_id)>0:
        content += '<div class="tipimgdiv"><img src="https://pubchem.ncbi.nlm.nih.gov/image/imgsrv.fcgi?cid=' + pubchem_id + '&amp;t=l" /></div>'
    content += '<p><strong>' + compound + ', ' + chembl 
    if len(pubchem_id)>0:
        content += ', ' + '<a href="https://pubchem.ncbi.nlm.nih.gov/compound/' + pubchem_id + '" >PubChem-' + pubchem_id + '</a>'
    content += '</strong></p><p>Total number of clinical trials mentioning this compound: '
    content += str(size) + '</p>'
    return content

def main():
    pubchem_mapping = load_pubchem_mapping() 
    col = db['stat']
    doclist = col.find()
    unknown = {'name' : 'unknown', 'children' :[]}
    jobj = {'name': 'statistics', 'children' :[unknown]}
    for doc in doclist:
        if doc['name'] == 'chembl':
            identifiers = doc['data']['top200_identifiers']
            for idf in identifiers:
                for key in idf:
                    record = db['chembl_dict'].find_one({'key': key})
                    compound = record['words'][0]
                    pubchem_id = ""
                    if key in pubchem_mapping.keys():
                        pubchem_id = pubchem_mapping[key]
                    clusters = get_clusters(pubchem_id)
                    size = idf[key]['trials']
                    leaf = {'chembl':key, 'name':compound, 'size': size} 
                    if len(pubchem_id)>0:
                        leaf['pubchem'] = pubchem_id
                    leaf['tooltip'] = get_tooltip(pubchem_id, compound, key, size)
                    if clusters:
                        for cltr in clusters:
                            l1obj = None
                            exist = [x for x in jobj["children"] if x['name'] == cltr]
                            if len(exist) > 0:
                                l1obj = exist[0]
                            else:
                                l1obj = {"name": cltr, "children": []}
                                jobj["children"].append(l1obj)
                            l1obj["children"].append(leaf)        
                    else:
                        unknown["children"].append(leaf)                
    with open('chem_cluster.json', 'w') as outfile:
        json.dump(jobj, outfile)

   

if __name__ == "__main__":
    main()
