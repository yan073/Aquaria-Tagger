#!/bin/bash
docker run -d --name tagging \
  --restart unless-stopped \
  -v <this-folder>:/app \
  yan047/trial-sync-env:3 python -u /app/tag.py