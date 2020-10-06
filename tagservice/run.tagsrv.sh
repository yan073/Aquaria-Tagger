#!/bin/bash
docker run -d --network=host --name tagsrv \
  -v app.cfg:/app/tagger/app.cfg \
  yan047/trialtagger:4 flask run --host=0.0.0.0