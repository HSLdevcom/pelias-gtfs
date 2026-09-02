var logger = require('pelias-logger').get('pelias-GTFS');
var fs = require('fs');
var axios = require('axios');
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

function createImportPipeline(datadir, prefix, otpUrlArg) {
  logger.info('Importing GTFS stops from ' + datadir);

  var otpUrl = otpUrlArg || process.env.OTP_URL;

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
  var alertClosedStops = new Set(); // stops closed by an active NO_SERVICE alert
  var stopAlertSeverity = {}; // stop_id -> 'alert' | 'info'

  var timezone = process.env.GTFS_TIMEZONE || 'Europe/Helsinki';
  var now = new Date();
  var today = new Intl.DateTimeFormat('en-CA', {
    timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(now).replace(/-/g, '');
  var todayDow = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone, weekday: 'long'
  }).format(now).toLowerCase();

  var validRecordFilterStream = ValidRecordFilterStream.create();
  var documentStream = DocumentStream.create(translations, prefix, activeStops, stopRouteTypes, stopsWithServiceToday, stopsWithFutureService, alertClosedStops, stopAlertSeverity);
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

    // collect station route types and schedule statuses from child stops in a single pass
    // (alert status is intentionally not propagated to parent stations)
    Object.keys(stationStops).forEach(station => {
      const stops = stationStops[station];
      var hasServiceToday = false;
      var hasFutureService = false;
      stops.forEach(stop => {
        stopRouteTypes[station] = {
          ...stopRouteTypes[station],
          ...stopRouteTypes[stop]
        };
        if (stopsWithServiceToday[stop]) { hasServiceToday = true; }
        if (stopsWithFutureService[stop]) { hasFutureService = true; }
      });
      if (!stopsWithServiceToday.notDefined && hasServiceToday) {
        stopsWithServiceToday[station] = true;
      }
      if (!stopsWithFutureService.notDefined && hasFutureService) {
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

  // fetch active stop alerts from OTP and fill alertClosedStops/stopAlertSeverity; never rejects
  var fetchAlerts = function() {
    if (!otpUrl || !prefix) {
      logger.info('OTP URL or prefix not set, skipping alert status fetch');
      return Promise.resolve();
    }
    var query = '{ alerts(feeds: ["' + prefix + '"]) { alertEffect alertSeverityLevel ' +
      'effectiveStartDate effectiveEndDate entities { __typename ... on Stop { gtfsId } } } }';
    return axios({
      method: 'post',
      url: otpUrl,
      headers: { 'Content-Type': 'application/graphql' },
      timeout: 10000,
      data: query
    }).then(function(res) {
      // GraphQL errors surface here with HTTP 200, not as a rejected request
      if (res.data && res.data.errors && res.data.errors.length) {
        logger.error('OTP returned GraphQL errors: ' + JSON.stringify(res.data.errors));
      }
      var nowUnixTime = Math.floor(Date.now() / 1000);
      var alerts = (res.data && res.data.data && res.data.data.alerts) || [];
      var stopIdPrefix = prefix + ':';
      var matchedStopCount = 0;
      alerts.forEach(function(alert) {
        // null start/end dates mean no restriction, not "always invalid"
        if ((alert.effectiveStartDate != null && alert.effectiveStartDate > nowUnixTime) ||
            (alert.effectiveEndDate != null && alert.effectiveEndDate < nowUnixTime)) {
          return;
        }
        (alert.entities || []).forEach(function(entity) {
          if (entity.__typename !== 'Stop' || !entity.gtfsId || entity.gtfsId.indexOf(stopIdPrefix) !== 0) {
            return;
          }
          var stopId = entity.gtfsId.substring(stopIdPrefix.length);
          matchedStopCount++;
          if (alert.alertEffect === 'NO_SERVICE') {
            alertClosedStops.add(stopId);
          } else {
            var severity = alert.alertSeverityLevel === 'INFO' ? 'info' : 'alert';
            if (severity === 'alert' || !stopAlertSeverity[stopId]) {
              stopAlertSeverity[stopId] = severity;
            }
          }
        });
      });
      logger.info('Fetched ' + alerts.length + ' alerts from OTP, matched ' + matchedStopCount +
        ' stop entities, ' + alertClosedStops.size + ' stops closed, ' +
        Object.keys(stopAlertSeverity).length + ' stops with alert severity');
    }).catch(function(err) {
      logger.error('Failed to fetch alerts from OTP: ' + err.message);
    });
  };

  var startImport = function() {
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
  };

  logger.info('Waiting for WOF setup');
  var wofDelay = new Promise(function(resolve) { setTimeout(resolve, 4000); });
  Promise.all([fetchAlerts(), wofDelay]).then(startImport);
}

module.exports = {
  create: createImportPipeline
};
