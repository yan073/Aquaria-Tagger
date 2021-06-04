from visual_protein import generate_protein_visual_data
from visual_chem import generate_chembl_visual_data, generate_pdb_visual_data, generate_pubchem_visual_data

def save_visual_data(v_data, context):
    vf_collection = context['visualfile_collection']
    vf_collection.update_one({'name':v_data['name'], 'file': v_data['file']}, \
                    {"$set": v_data }, upsert=True)

def generate_visual_data(trial_tags, context):
    v_data = None
    doclist = context['stat_collection'].find()
    for doc in doclist:
        if doc_name == 'protein':
            v_data = generate_protein_visual_data(trial_tags, doc, context)
        elif doc_name == 'chembl':
            v_data = generate_chembl_visual_data(trial_tags, doc, context)
        elif doc_name == 'pdb':
            v_data = generate_pdb_visual_data(trial_tags, doc, context)
        elif doc_name == 'pubchem':
            v_data = generate_pubchem_visual_data(trial_tags, doc, context)
        else:
            v_data = None
            print (f'Error: unimplemented statistics for dictionary {doc_name}')
        if v_data is not None:
            save_visual_data(v_data, context)


