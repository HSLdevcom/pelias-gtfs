# GTFS import pipeline

A tool for importing [GTFS](https://developers.google.com/transit/gtfs/) transit stops into Pelias.

## Install dependencies

```bash
npm install
```

## Usage

`node import.js -d /path-to-gtfs-data/ --prefix=xxx`: run the data import using the given data path

In above, the optional prefix will be added to the full document id as 'GTFS:<prefix>:stop_id'.

Zipped data can be dowloaded from: http://api.digitransit.fi/routing-data/v2/hsl/HSL.zip
