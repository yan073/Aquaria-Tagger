
# Tagging Service 

Tag the requesting text with the dictionaries, blacklists and whitelists loaded from database.

### build the docker image yan047/trialtagger:4
Use the Dockerfile in this folder.

### Protein and chemical dictionaries
Load protein and chemical dictionaries into Mongodb database

### Config information
load src/config.json into Mongodb collection 'config'

### Start the tagging service

```bash
./run.tagsrv.sh
```
