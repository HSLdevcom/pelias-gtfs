var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );
var logger = require( 'pelias-logger' ).get( 'pelias-GTFS' );

function mapVehicleType(type) {
  switch(type) {
  case '0':
    return ['raitiovaunupysäkki', 'spårvagnsstop', 'tram stop'];
  case '1':
    return ['metroasema', 'tunnelbanestation', 'subway station'];
  case '3':
    return ['bussipysäkki', 'busshållplats', 'bus stop'];
  case '4':
    return ['lauttalaituri', 'färjan kaj', 'ferry pier'];
  case '109':
    return ['junapysäkki', 'järnvägsstation', 'train stop'];
  }

  return null;
}

/*
 * Create a stream of Documents from valid CSV records
 */
function createDocumentStream(translations) {

  var badRecordCount=0;

  return through.obj(
    function write( record, enc, next ){
      try {
        var model_id = 'GTFS' + ':' + record.stop_id;
        var name = record.stop_name;
        var stopCode;
        if (record.stop_code && record.stop_code !== '') {
          stopCode = ', ' + record.stop_code;
        } else {
          stopCode = '';
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
        .setCentroid( { lon: record.stop_lon, lat: record.stop_lat } );

        doc.setName( 'fi', fullName );

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
        var vehicleTypes = mapVehicleType(record.vehicle_type);
        if (vehicleTypes) {
          vehicleTypes.forEach( function (type) {
            doc.addCategory(type);
          });
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
