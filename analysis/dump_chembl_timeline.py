from pymongo import MongoClient
import json
from datetime import datetime, timedelta, date
from operator import itemgetter


dburi = "mongodb://localhost/"
dbname = 'nihtrial'
STAT_COLLECTION = 'stat'

c = MongoClient( dburi )
db = c[ dbname ]
trial_collection = db['trial']

trialfields = {'ctid': True, 'untagged.firstSubmittedDate': True, \
    'untagged.firstPostedDate' : True, 'chembl_key': True, 'pdb_key': True, \
    'primary_accession': True, 'pubchem_cid': True, '_id': False}
tagdocs = []
for doc in trial_collection.find(filter=None, projection=trialfields ):
    tagdocs.append(doc)

with open('trial_tags.json', 'w') as outfile:
    json.dump(tagdocs, outfile)

for doc in tagdocs:
    doc['submitted'] =  datetime.strptime(doc['untagged']['firstSubmittedDate'], '%B %d, %Y').date()
    doc['posted'] = datetime.strptime(doc['untagged']['firstPostedDate'], '%B %d, %Y').date()

latest = max(tagdocs, key=itemgetter('submitted'))['submitted']

start = date.fromisoformat("2019-10-01")

period_offset = 31

periods = []

while start < latest:
    end = (start + timedelta(days=period_offset)).replace(day = 1 )
    if end <= latest:
        periods.append({'start': start, 'end': end})
    start = end

timeline = []

for period in periods:
    trials = [x for x in tagdocs if period['end'] > x['submitted'] >= period['start']]
    chembls1 = {}
    for doc in trials:
        for ckey in doc['chembl_key']:
            c_trial = chembls1.get(ckey, None)
            if c_trial: 
                chembls1[ckey] += 1
            else:
                chembls1[ckey] = 1
    timeline.append( {'start': period['start'].isoformat(), 'chembl' : chembls1} )

chembls = set()
for entry in timeline:
    for key in entry['chembl'].keys():
        chembls.add(key)

from datetime import datetime, timedelta, date
start = date.fromisoformat("2019-10-01")
latest =  date.fromisoformat("2021-04-01")
period_offset = 31 
import csv

with open('chembl_timeline.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',',quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['key','value','date])
    while start < latest:
        records = [x for x in timeline if date.fromisoformat(x['start']) == start]
        for r in records:
            for ch in chembls:
                cc = r["chembl"].get(ch, 0)
                writer.writerow([ch, cc, start.isoformat()])
        start = (start + timedelta(days=period_offset)).replace(day = 1 )

