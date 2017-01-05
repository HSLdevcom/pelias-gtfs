var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );
var logger = require( 'pelias-logger' ).get( 'pelias-GTFS' );

function mapVehicleType(type) {
  switch(type) {
  case '0':
    return 'tram stop';
  case '1':
    return 'subway station';
  case '3':
    return 'bus stop';
  case '4':
    return 'ferry pier';
  case '109':
    return 'train stop';
  }

  return null;
}

/*
 * Create a stream of Documents from valid CSV records
 */
function createDocumentStream(translations, prefix) {

  var badRecordCount=0;
  var fullPrefix = 'GTFS:';

  if(prefix) {
    fullPrefix += prefix + ':'; // e.g. 'GTFS:HSL:'
  }
  return through.obj(
    function write( record, enc, next ){
      try {
        var model_id = fullPrefix + record.stop_id;
        var name = record.stop_name;
        var stopCode = '';
        var shortName;
        if (record.stop_code && record.stop_code !== '') {
          shortName = record.stop_code;
          if (name.indexOf(record.stop_code)===-1) {
            // append to full name if not already appended
            stopCode = ' ' + record.stop_code;
          }
        }
        var fullName = name + stopCode;
        var type;
        if (record.location_type === '1') {
          type = 'station';
        } else {
          type = 'stop';
        }
        var doc = new peliasModel.Document( 'GTFS', type, model_id )
            .setName( 'default', fullName )
            .setName( 'fi', fullName )
            .setCentroid( { lon: record.stop_lon, lat: record.stop_lat } );

        if(shortName && shortName !== fullName) {
          doc.setName( 'short', shortName );
        }

        if (translations) {
          var names = translations[name];
          if (names) {
            for (var lang in names) {
              var name2 = names[lang];
              if (name2 !== name) {
                doc.setName( lang, name2 + stopCode );
              }
            }
          }
        }
        var vehicleType = mapVehicleType(record.vehicle_type);
        if (vehicleType) {
          doc.addCategory(vehicleType);
        }
        if (type === 'station') {
          doc.setPopularity(1000000);
        } else {
          doc.setPopularity(10000);
        }

        this.push( doc );
      }
      catch ( ex ){
        badRecordCount++;
      }

      next();
    }, function end( done ) {
      logger.info('Bad record count: ' + badRecordCount);
      done();
    }
  );
}

module.exports = {
  create: createDocumentStream
};
