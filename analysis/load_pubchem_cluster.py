from pymongo import MongoClient
import csv
from datetime import datetime

dburi = "mongodb://localhost/"
dbname = "covidtag"

c = MongoClient( dburi )
db = c[ dbname ]

cluster_size = 64
cluster_ids = ['U.Clus.' + str(x) for x in list(range(1, cluster_size+1))]

clusters = []

with open('results-m1_15-k64.csv', newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    line_count = 0
    for row in csv_reader:
        line_count += 1
        if line_count > 1:
            record = {'pubchem_id' : row[0], 'distance' :[], 'clusters' :[]}
            for num, cid in enumerate(cluster_ids, start=1):
                distance = float(row[num])
                record['distance'].append( {'id': cid, 'distance': distance})
                if distance > 0.25:
                    record['clusters'].append(cid)
            clusters.append (record)

new_doc = {'name': 'pubchem_cluster', 'data': clusters, 'timestamp': datetime.now(), 'notes' :'m1_15-k64', 'distance_threshold':0.25}
db['chemical_cluster'].insert_one(new_doc)

