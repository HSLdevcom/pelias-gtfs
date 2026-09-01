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
  var translationFile = path.join(datadir, 'translations.txt');
  var stopTimesFile = path.join(datadir, 'stop_times.txt');
  var tripsFile = path.join(datadir, 'trips.txt');
  var routesFile = path.join(datadir, 'routes.txt');
  var calendarFile = path.join(datadir, 'calendar.txt');
  var calendarDatesFile = path.join(datadir, 'calendar_dates.txt');

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
  var calendarParser = csvParse(csvOptions);
  var calendarDatesParser = csvParse(csvOptions);
  var translations = {};
  var activeStops = {};
  var tripRoutes = {};
  var tripServiceIds = {};
  var routeModes = {};
  var stopRouteTypes = {};
  var stationStops = {};
  var activeServiceIds = new Set();
  var futureServiceIds = new Set();
  var stopsWithServiceToday = { notDefined: true };
  var stopsWithFutureService = { notDefined: true };

  var timezone = process.env.GTFS_TIMEZONE || 'Europe/Helsinki';
  var now = new Date();
  var today = new Intl.DateTimeFormat('en-CA', {
    timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(now).replace(/-/g, '');
  var todayDow = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone, weekday: 'long'
  }).format(now).toLowerCase();

  var validRecordFilterStream = ValidRecordFilterStream.create();
  var documentStream = DocumentStream.create(translations, prefix, activeStops, stopRouteTypes, stopsWithServiceToday, stopsWithFutureService);
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
      const key = record.field_value || record.record_id;
      if (!translations[key]) {
        translations[key] = {};
      }
      var stopTranslation = translations[key];
      stopTranslation[record.language] = record.translation;
    }
    next();
  }, function (done) {
    logger.info('Translations loaded, launch stop import');
    documentReader(stopsFile);

    done();
  });

  var importWithTranslations = function() {
    if (fs.existsSync(translationFile)) {
      logger.info('Found translations');
      fs.createReadStream(translationFile)
        .pipe(translationParser)
        .pipe(translationCollector);
    } else {
      documentReader(stopsFile);
    }
  };

  var startRouteProcessing = function() {
    fs.createReadStream(routesFile)
      .pipe(routeParser)
      .pipe(routeCollector);
  };

  var calendarDatesCollector = through.obj(function(record, enc, next) {
    if (record.date === today) {
      if (record.exception_type === '1') activeServiceIds.add(record.service_id);
      else if (record.exception_type === '2') activeServiceIds.delete(record.service_id);
    }
    if (record.date >= today && record.exception_type === '1') {
      futureServiceIds.add(record.service_id);
    }
    next();
  }, function(done) {
    logger.info('Calendar exceptions analyzed');
    startRouteProcessing();
    done();
  });

  var calendarCollector = through.obj(function(record, enc, next) {
    if (record[todayDow] === '1' && record.start_date <= today && record.end_date >= today) {
      activeServiceIds.add(record.service_id);
    }
    var hasWeekdayService = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
      .some(function(day) { return record[day] === '1'; });
    if (record.end_date >= today && hasWeekdayService) {
      futureServiceIds.add(record.service_id);
    }
    next();
  }, function(done) {
    logger.info('Calendar analyzed, active service IDs: ' + activeServiceIds.size);
    if (fs.existsSync(calendarDatesFile)) {
      fs.createReadStream(calendarDatesFile)
        .pipe(calendarDatesParser)
        .pipe(calendarDatesCollector);
    } else {
      startRouteProcessing();
    }
    done();
  });

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
      if (!stopsWithServiceToday.notDefined && stops.some(stop => stopsWithServiceToday[stop])) {
        stopsWithServiceToday[station] = true;
      }
      if (!stopsWithFutureService.notDefined && stops.some(stop => stopsWithFutureService[stop])) {
        stopsWithFutureService[station] = true;
      }
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
    var serviceId = tripServiceIds[record.trip_id];
    if (!stopsWithServiceToday.notDefined && activeServiceIds.has(serviceId)) {
      stopsWithServiceToday[record.stop_id] = true;
    }
    if (!stopsWithFutureService.notDefined && futureServiceIds.has(serviceId)) {
      stopsWithFutureService[record.stop_id] = true;
    }
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
    tripServiceIds[record.trip_id] = record.service_id;
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
      if (fs.existsSync(calendarFile)) {
        delete stopsWithServiceToday.notDefined;
        delete stopsWithFutureService.notDefined;
        fs.createReadStream(calendarFile)
          .pipe(calendarParser)
          .pipe(calendarCollector);
      } else if (fs.existsSync(calendarDatesFile)) {
        delete stopsWithServiceToday.notDefined;
        delete stopsWithFutureService.notDefined;
        fs.createReadStream(calendarDatesFile)
          .pipe(calendarDatesParser)
          .pipe(calendarDatesCollector);
      } else {
        startRouteProcessing();
      }
    } else {
      activeStops.notDefined = true;
      importWithTranslations();
    }
  }, 4000);
}

module.exports = {
  create: createImportPipeline
};
