var logger = require( 'pelias-logger' ).get( 'pelias-GTFS' );
var fs = require( 'fs' );
var csvParse = require( 'csv-parse' );
var ValidRecordFilterStream = require('./validRecordFilterStream');
var DocumentStream = require('./documentStream');
var AdminLookupStream = require('./adminLookupStream');
var model = require( 'pelias-model' );
var peliasDbclient = require( 'pelias-dbclient' );
var through = require( 'through2' );
var path = require('path');

/**
 * Import GTFS stops ( a CSV file ) in a directory into Pelias elasticsearch.
 *
 * @param dir  Path to a directory containing GTFS stops.txt and optionally translations.txt
 *
 */

function createImportPipeline( datadir ) {
  logger.info( 'Importing GTFS stops from ' + datadir );

  var fileName = path.join(datadir, 'stops.txt');
  var translationFile = path.join(datadir, 'translations.txt');

  var csvOptions = {
    trim: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax: true,
    columns: true
  };

  var stopParser = csvParse(csvOptions);
  var translationParser = csvParse(csvOptions);
  var translations = {};

  var validRecordFilterStream = ValidRecordFilterStream.create();
  var documentStream = DocumentStream.create(translations);
  var adminLookupStream = AdminLookupStream.create();
  var finalStream = peliasDbclient({});

  var collector = through.obj( function ( record, enc, next ) {
    if (record.lang !== 'fi') {
      if ( !translations[record.trans_id] ) {
        translations[record.trans_id] = {};
      }
      var translation = translations[record.trans_id];
      if(translation !== record.trans_id) {
        translation[record.lang] = record.translation;
      }
    }
    next();
  }, function ( done ) {
    logger.info('---------------------------------------');
    logger.info('Translations loaded, launch stop import');
    logger.info('---------------------------------------');

    fs.createReadStream( fileName ) // create the main stream
      .pipe( stopParser )
      .pipe( validRecordFilterStream )
      .pipe( documentStream )
      .pipe( adminLookupStream )
      .pipe( model.createDocumentMapperStream() )
      .pipe( finalStream );
    done();
  });

  fs.createReadStream( translationFile )
    .pipe( translationParser )
    .pipe( collector );
}

module.exports = {
  create: createImportPipeline
};
