import tagger
import datetime

DOCUMENT_ID = 'doc'
PRIMARY_ACCESSION = 'primary_accession'
TIMESTAMP_ELEMENT = 'timestamp'
WORDS_ELEMENT = 'words'
DEFAULT_TIMESTAMP = datetime.datetime(2020, 1, 1)
str_DEFAULT_TIMESTAMP = DEFAULT_TIMESTAMP.isoformat()
def reload_new_dictionaries(context, taggers):
    db = context['db']
    config_collection = context['config_collection']
    dict_defs = list ( db[config_collection].find( {'class':'dictionary'} ))
    logger = context['logger']
    for ddef in dict_defs:
        try:
            name = ddef['name']
            search = [x for x in taggers if x['name'] == name]
            re_create = True
            if len(search) > 0:
                t_inst = search[0]
                if t_inst['blacklist_timestamp'] == get_last_timestamp(ddef['blacklist'], db) \
                    and t_inst['whitelist_timestamp'] == get_last_timestamp(ddef['whitelist'], db):
                    re_create = False
                else:
                    taggers.remove(t_inst)
            if re_create:
                if ddef['name'] =='protein':
                    taggers.append(create_protein_tagger(ddef, context))
                else:
                    taggers.append(create_chem_tagger(ddef, context))  
                msg = '\tdictionary ' + name + ' is reloaded!'
                logger.info(msg)          
        except Exception as e:
            logger.error(e)

def get_last_timestamp(col_name, db):
    if len(col_name) >0:
        latest = db[col_name].find().sort([(TIMESTAMP_ELEMENT, -1)]).limit(1)
        if (latest.count()>0) :
            return latest.next()[TIMESTAMP_ELEMENT].isoformat()
    return str_DEFAULT_TIMESTAMP

def create_taggers(context):
    db = context['db']
    config_collection = context['config_collection']
    dict_defs = list ( db[config_collection].find( {'class':'dictionary'} ))
    taggers = []
    for ddef in dict_defs:
        if ddef['name'] =='protein':
            taggers.append(create_protein_tagger(ddef, context))
        else :
            taggers.append(create_chem_tagger(ddef, context))
    return taggers

def create_tgr(name, engine, entity_types, black_timestamp, white_timestamp):
    tgr_inst = {'name': name, 'engine' : engine, 'entity_types': entity_types}
    if black_timestamp:
        tgr_inst['blacklist_timestamp'] = black_timestamp
    else:
        tgr_inst['blacklist_timestamp'] = ''
    if white_timestamp:
        tgr_inst['whitelist_timestamp'] = white_timestamp
    else:
        tgr_inst['whitelist_timestamp'] = ''
    return tgr_inst
    
def create_protein_tagger(ddef, context):
    logger = context['logger']
    logger.info('create tagger for dictionary ' + ddef['name'])
    engine, black_timestamp, white_timestamp = create_protein_engine(ddef, context)
    entity_types = set([ddef['entity_type']])
    return create_tgr(ddef['name'], engine, entity_types, black_timestamp, white_timestamp)

def create_chem_tagger(ddef, context):
    logger = context['logger']
    logger.info('create tagger for dictionary ' + ddef['name'])
    engine, black_timestamp, white_timestamp = create_chemical_engine(ddef, context)
    entity_types = set([ddef['entity_type']])
    return create_tgr(ddef['name'], engine, entity_types, black_timestamp, white_timestamp)

def create_protein_engine(ddef, context):
    db = context['db']
    logger = context['logger']
    logger.info('preparing protein engine...')
    dict_c_name = ddef['dictionary_collection']
    logger.info('\tdictionary collection name is: ' + dict_c_name) 
    blacklist_name = ddef['blacklist']
    logger.info('\tblacklist collection name is: ' + blacklist_name) 
    whitelist_name = ddef['whitelist'] 
    logger.info('\twhitelist collection name is: ' + whitelist_name)    
    entity_type = ddef['entity_type']

    tgr = tagger.Tagger()
    for edict in db[dict_c_name].find():
        for entry in edict['dictionary']:
            p_a = entry[PRIMARY_ACCESSION].encode("utf-8")
            for word in entry['words']:
                tgr.add_name(word.encode("utf-8"), entity_type, p_a)

    black_timestamp = tagger_block_blacklist(db[blacklist_name], tgr)
    white_timestamp = str_DEFAULT_TIMESTAMP
    wlitems = ddef.get('whitelist_items', None)
    if len(whitelist_name) > 0 and wlitems:
        white_timestamp = tagger_add_whitelist(db[whitelist_name], tgr, entity_type,  \
            wlitems[0], wlitems[1])
    return tgr, black_timestamp, white_timestamp

def tagger_block_blacklist(blist_c, tgr):
    # get blacklist
    blacklist = {}
    latest = blist_c.find().sort([(TIMESTAMP_ELEMENT, -1)]).limit(1)
    if (latest.count()>0) :
        blacklist = latest.next()
    else :
        blacklist[WORDS_ELEMENT] = ''
        blacklist[TIMESTAMP_ELEMENT] = DEFAULT_TIMESTAMP
    # blocking
    for word in blacklist[WORDS_ELEMENT]:
        tgr.block_name(word.encode("utf-8"), DOCUMENT_ID)
    return blacklist[TIMESTAMP_ELEMENT].isoformat()

def tagger_add_whitelist(wlist_c, tgr, entity_type, key_name, word_name):
    # get whitelist
    whitelist = {}
    latest = wlist_c.find().sort([(TIMESTAMP_ELEMENT, -1)]).limit(1)
    if (latest.count()>0) :
        whitelist = latest.next()
    else :
        whitelist['dictionary'] = []
        whitelist[TIMESTAMP_ELEMENT] = DEFAULT_TIMESTAMP
    # adding new words in whitelist
    for grp in whitelist['dictionary']:
        c_key = grp[key_name].encode("utf-8")
        for word in grp[word_name]:
            tgr.add_name(word.encode("utf-8"), entity_type, c_key)
    return whitelist[TIMESTAMP_ELEMENT].isoformat()

def create_chemical_engine(ddef, context):
    db = context['db']
    logger = context['logger']    
    logger.info ('preparing chemical engine...')
    dict_c_name = ddef['dictionary_collection']
    logger.info ('\tdictionary collection name is: ' + dict_c_name) 
    blacklist_name = ddef['blacklist']
    logger.info ('\tblacklist collection name is: ' + blacklist_name) 
    whitelist_name = ddef['whitelist'] 
    logger.info ('\twhitelist collection name is: ' + whitelist_name)
    entity_type = ddef['entity_type']
    tgr = tagger.Tagger()
    if len(dict_c_name) > 0:
        for entry in db[dict_c_name].find():
            c_key = entry['key'].encode("utf-8")
            for word in entry['words']:
                tgr.add_name(word.encode("utf-8"), entity_type, c_key)
    black_timestamp = str_DEFAULT_TIMESTAMP
    if len(blacklist_name) > 0:
        black_timestamp = tagger_block_blacklist(db[blacklist_name], tgr)
    white_timestamp = str_DEFAULT_TIMESTAMP
    wlitems = ddef.get('whitelist_items', None)
    if len(whitelist_name) > 0 and wlitems:
        white_timestamp = tagger_add_whitelist(db[whitelist_name], tgr, entity_type, \
                wlitems[0], wlitems[1])
    logger.info ('\tload chemical dictionary completed.')
    return tgr, black_timestamp, white_timestamp
