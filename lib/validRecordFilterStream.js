var through = require( 'through2' );
var logger = require( 'pelias-logger' ).get( 'pelias-GTFS' );

function isValidCsvRecord( record ) {
  return [ 'stop_name', 'stop_lat', 'stop_lon' ].every(function(prop) {
    return record[ prop ] && record[ prop ].length > 0;
  });
}

/*
 * filter out invalid records
 */
function createValidRecordFilterStream() {
  var invalidCount = 0;

  return through.obj(function( record, enc, next ) {
    if (isValidCsvRecord(record)) {
      this.push(record);
    } else {
      invalidCount++;
    }
    next();
  }, function(next) {
    logger.verbose('Skipped invalid records: ' + invalidCount);
    next();
  });
}

module.exports = {
  create: createValidRecordFilterStream
};
