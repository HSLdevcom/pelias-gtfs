var logger = require('pelias-logger').get('pelias-GTFS');
var fs = require('fs');
var csvParse = require('csv-parse').parse;
var ValidRecordFilterStream = require('./validRecordFilterStream');
var DocumentStream = require('./documentStream');
var AdminLookupStream = require('pelias-wof-admin-lookup');
var model = require('pelias-model');
var peliasDbclient = require('pelias-dbclient');
var through = require('through2');
var path = require('path');

/**
 * Import GTFS stops (a CSV file) in a directory into Pelias elasticsearch.
 *
 * @param dir  Path to a directory containing GTFS stops.txt and optionally translations.txt
 *
 */

function createImportPipeline(datadir, prefix) {
  logger.info('Importing GTFS stops from ' + datadir);

  var stopsFile = path.join(datadir, 'stops.txt');
  var translationFile = path.join(datadir, 'translations_new.txt');
  var stopTimesFile = path.join(datadir, 'stop_times.txt');
  var tripsFile = path.join(datadir, 'trips.txt');
  var routesFile = path.join(datadir, 'routes.txt');

  var csvOptions = {
    bom: true,
    trim: true,
    skip_empty_lines: true,
    columns: hdr => {
      // !!! for some reason HSL GTFS stop_times.txt header contains bad chars, must trim
      return hdr.map(key => key.trim());
    }
  };
  var routeParser = csvParse(csvOptions);
  var tripParser = csvParse(csvOptions);
  var stopParser = csvParse(csvOptions);
  var parentStopParser = csvParse(csvOptions);
  var translationParser = csvParse(csvOptions);
  var stopTimesParser = csvParse(csvOptions);
  var translations = {};
  var activeStops = {};
  var tripRoutes = {};
  var routeModes = {};
  var stopRouteTypes = {};
  var stationStops = {};

  var validRecordFilterStream = ValidRecordFilterStream.create();
  var documentStream = DocumentStream.create(translations, prefix, activeStops, stopRouteTypes);
  var adminLookupStream = AdminLookupStream.create();
  var finalStream = peliasDbclient({});

  var documentReader = function(stopsFile) {
    fs.createReadStream(stopsFile) // create the main stream
      .pipe(stopParser)
      .pipe(validRecordFilterStream)
      .pipe(documentStream)
      .pipe(adminLookupStream)
      .pipe(model.createDocumentMapperStream())
      .pipe(finalStream);
  };

  // extract stop name translations
  var translationCollector = through.obj(function (record, enc, next) {
    if(record.table_name === 'stops' && record.field_name === 'stop_name') {
      if (!translations[record.field_value]) {
	translations[record.field_value] = {};
      }
      var stopTranslation = translations[record.field_value];
      stopTranslation[record.language] = record.translation;
    }
    next();
  }, function (done) {
    logger.info('Translations loaded, launch stop import');
    documentReader(stopsFile);

    done();
  });

  var importWithTranslations = function() {
    if (!fs.existsSync(translationFile)) { // TODO always use translations.txt after format transition
      translationFile = path.join(datadir, 'translations.txt');
    }
    if (fs.existsSync(translationFile)) {
      fs.createReadStream(translationFile)
        .pipe(translationParser)
        .pipe(translationCollector);
    } else {
      documentReader(stopsFile);
    }
  };

  // mark stations which have child stops active
  var parentStopCollector = through.obj(function (record, enc, next) {
    if (record.parent_station) {
      const parent_id = record.parent_station;
      if(activeStops[record.stop_id]) {
	// active stop activates parent station
	activeStops[parent_id] = true;
	if(!stationStops[parent_id]){
          stationStops[parent_id] = [];
	}
	stationStops[parent_id].push(record.stop_id);
      }
    }
    next();
  }, function (done) {
    logger.info('Parent station references analyzed');

    // collect station route types from child stops
    Object.keys(stationStops).forEach(station => {
      const stops = stationStops[station];
      stops.forEach(stop => {
        stopRouteTypes[station] = {
          ...stopRouteTypes[station],
          ...stopRouteTypes[stop]
        };
      });
    });
    importWithTranslations();

    done();
  });

  // mark stops through which trips travel active, and extract route types (=transport modes in OTP terms)
  var activeStopCollector = through.obj(function (record, enc, next) {
    activeStops[record.stop_id] = true;
    if(!stopRouteTypes[record.stop_id]) {
      stopRouteTypes[record.stop_id] = {};
    }
    stopRouteTypes[record.stop_id][routeModes[tripRoutes[record.trip_id]]] = true;
    next();
  }, function (done) {
    logger.info('Stop references from stop_times analyzed');

    fs.createReadStream(stopsFile)
      .pipe(parentStopParser)
      .pipe(parentStopCollector);
    done();
  });

  // extract routes by trip map
  var tripCollector = through.obj(function (record, enc, next) {
    tripRoutes[record.trip_id] = record.route_id;
    next();
  }, function (done) {
    logger.info('Trips analyzed');

    fs.createReadStream(stopTimesFile)
      .pipe(stopTimesParser)
      .pipe(activeStopCollector);
    done();
  });

  // extract route modes
  var routeCollector = through.obj(function (record, enc, next) {
    routeModes[record.route_id] = record.route_type;
    next();
  }, function (done) {
    logger.info('Routes analyzed');
    fs.createReadStream(tripsFile)
      .pipe(tripParser)
      .pipe(tripCollector);
    done();
  });

  logger.info('Waiting for WOF setup');
  setTimeout(function() {
    logger.info('Start import');
    if (fs.existsSync(routesFile) && fs.existsSync(tripsFile) && fs.existsSync(stopTimesFile)) {
      // enrich data by scanning active stops and stations and their transport modes
      fs.createReadStream(routesFile)
        .pipe(routeParser)
        .pipe(routeCollector);
    } else {
      activeStops.notDefined = true;
      importWithTranslations();
    }
  }, 4000);
}

module.exports = {
  create: createImportPipeline
};
