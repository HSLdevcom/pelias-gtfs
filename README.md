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

### Stop alert statuses

`--otpUrl=<OTP graphql endpoint>` (or the `OTP_URL` environment variable) enables fetching active stop
alerts from OTP and adding them to each stop's `addendum.GTFS` as `noService`/`alertSeverity`, alongside
the existing schedule-based statuses. This assumes `--prefix` matches the OTP feed id. If neither is set,
alert fetching is skipped and the import behaves as before.

Include the Digitransit API subscription key directly in `--otpUrl` as a query parameter.
