
# Tag Clinical Trials 

Tag trial text stored in Mongodb database with remote tagging service

### Build the docker image yan047/trial-sync-env:3
Use the Dockerfile in this folder.

### Start tagging trial records

Start the tagging with docker image:

```bash
./run.tag.sh
```

Start the tagging with python command:

```bash
python tag.py
```
