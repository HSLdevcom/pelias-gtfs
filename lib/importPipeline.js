var logger = require( 'pelias-logger' ).get( 'pelias-GTFS' );
var fs = require( 'fs' );
var csvParse = require( 'csv-parse' );
var ValidRecordFilterStream = require('./validRecordFilterStream');
var DocumentStream = require('./documentStream');
var AdminLookupStream = require('./adminLookupStream');
var model = require( 'pelias-model' );
var peliasDbclient = require( 'pelias-dbclient' );
var through = require( 'through2' );


/**
 * Import GTFS stops ( a CSV file ) in a directory into Pelias elasticsearch.
 *
 * @param path  Path to a directory containing GTFS stops.txt and optionally translations.txt
 *
 */

function createImportPipeline( path ) {
  logger.info( 'Importing GTFS stops from ' + path );

  if (!path.endsWith('/')) {
    path = path + '/';
  }
  var fileName = path + 'stops.txt';
  var translationFile = path + 'translations.txt';

  var csvParser = csvParse({
    trim: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax: true,
    columns: true
  });

  var csvParser2 = csvParse({
    trim: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax: true,
    columns: true
  });

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
      .pipe( csvParser2 )
      .pipe( validRecordFilterStream )
      .pipe( documentStream )
      .pipe( adminLookupStream )
      .pipe( model.createDocumentMapperStream() )
      .pipe( finalStream );
    done();
  });

  fs.createReadStream( translationFile )
    .pipe( csvParser )
    .pipe( collector );
}

module.exports = {
  create: createImportPipeline
};
