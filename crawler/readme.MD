# Crawler to capture all the COVID-19 related trial records from the web site https://clinicaltrials.gov/

------------

### Compile

```bash
  npm run compile
```

### Run

```bash
  node .
```

### Run with docker image

```bash
docker run --rm -v /crawler/src/.env:/app/.env <image-name> node /app
```