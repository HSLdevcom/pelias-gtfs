var logger = require('pelias-logger').get('pelias-GTFS');
var fs = require('fs');
var csvParse = require('csv-parse');
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

  var fileName = path.join(datadir, 'stops.txt');
  var translationFile = path.join(datadir, 'translations3.txt');
  var stopTimesFile = path.join(datadir, 'stop_times.txt');

  var csvOptions = {
    trim: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax: true,
    columns: true
  };

  var stopParser = csvParse(csvOptions);
  var parentStopParser = csvParse(csvOptions);
  var translationParser = csvParse(csvOptions);
  var stopTimesParser = csvParse(csvOptions);
  var translations = {};
  var activeStops = {};

  var validRecordFilterStream = ValidRecordFilterStream.create();
  var documentStream = DocumentStream.create(translations, prefix, activeStops);
  var adminLookupStream = AdminLookupStream.create();
  var finalStream = peliasDbclient({});

  var documentReader = function(fileName) {
    fs.createReadStream(fileName) // create the main stream
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
	  if (!translations[record.record_id]) {
	      translations[record.record_id] = {};
	  }
	  var stopTranslation = translations[record.record_id];
          stopTranslation[record.language] = record.translation;
      }
      next();
  }, function (done) {
    logger.info('Translations loaded, launch stop import');
    documentReader(fileName);

    done();
  });

  // mark stops through which trips travel active
  var activeStopCollector = through.obj(function (record, enc, next) {
    activeStops[record.stop_id] = true;
      next();
  }, function (done) {
    logger.info('Stop references analyzed');

    if (!fs.existsSync(translationFile)) { // TODO always use translations.txt after format transition
      translationFile = path.join(datadir, 'translations.txt');
    }
    if (fs.existsSync(translationFile)) {
      fs.createReadStream(translationFile)
	.pipe(translationParser)
	.pipe(translationCollector);
    } else {
      documentReader(fileName);
    }
    done();
  });

  // mark stations which have child stops active
  var parentStopCollector = through.obj(function (record, enc, next) {
    if (record.parent_station) {
      activeStops[record.parent_station] = true;
    }
    next();
  }, function (done) {
    logger.info('Parent station references analyzed');
    fs.createReadStream(stopTimesFile)
      .pipe(stopTimesParser)
      .pipe(activeStopCollector);
    done();
  });

  fs.createReadStream(fileName)
    .pipe(parentStopParser)
    .pipe(parentStopCollector);
}

module.exports = {
  create: createImportPipeline
};
