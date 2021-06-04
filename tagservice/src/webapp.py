from flask import Flask
from flask import request
from flask import jsonify

import logging

from pymongo import MongoClient

import tagdict

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

app.config.from_envvar('CONFIG_FILE')

app.logger.info('MONGODB_SERVER =' + app.config['MONGODB_SERVER'])
app.logger.info('MONGODB_DB =' + app.config['MONGODB_DB'])
app.logger.info('MONGODB_PORT =' + app.config['MONGODB_PORT'])
app.logger.info('MONGODB_CONFIG =' + app.config['MONGODB_CONFIG'])

c = MongoClient(app.config['MONGODB_SERVER'], int(app.config['MONGODB_PORT']))

context = {'db': c[app.config['MONGODB_DB']] , 'logger': app.logger, \
            'config_collection': app.config['MONGODB_CONFIG']}

taggers = tagdict.create_taggers(context)

DOCUMENT_ID = 'doc'
QUERY_DOC = 'doc'
QUERY_DICTIONARY = 'dict'

@app.route('/tag', methods=['POST'])
def tag():
    query = request.get_json(force = True)
    tgr = get_tagger(query[QUERY_DICTIONARY])
    response = {}
    if tgr:
        response['match'] = tag_text(tgr, query[QUERY_DOC])
    else:
        response = {'error': 'Unknown dictionary.'}
    return response

@app.route('/dictionaries/update', methods=['POST'])
def update_dictionaries():
    tagdict.reload_new_dictionaries(context, taggers)
    app.logger.info('All dictionaries are up-to-date!')
    return get_all_dicts_info()

@app.route('/dictionaries', methods=['GET'])
def dictionaries():
    return get_all_dicts_info()

def get_all_dicts_info():
    response = []
    for tgr in taggers:
        response.append( get_tagger_info(tgr) )
    return {'dictionaries': response}

def get_tagger_info(tgr):
    tgr_info = {'name': tgr['name'], 'entity_types':list(tgr['entity_types'])[0]}
    tgr_info['blacklist'] = tgr['blacklist_timestamp']
    tgr_info['whitelist'] = tgr['whitelist_timestamp']
    return tgr_info
    
def get_tagger(dict_name):
    for tgr in taggers:
        if tgr['name'] == dict_name:
            return tgr
    return None

def tag_text(tgr, text):
    engine = tgr['engine']
    return engine.get_matches(document=text, document_id=DOCUMENT_ID, entity_types= tgr['entity_types'])

if __name__ == '__main__':
    app.run()
