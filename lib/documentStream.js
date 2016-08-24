var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );

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
function createDocumentStream(stats, translations) {

  return through.obj(
    function write( record, enc, next ){
      try {
        var model_id = 'GTFS' + ':' + record.stop_id;
        var name = record.stop_name;
        var stopCode;
        if (record.stop_code && record.stop_code !== '') {
          stopCode = ' ( ' + record.stop_code + ' )';
        } else {
          stopCode = '';
        }
        var fullName = name + stopCode;

        var doc = new peliasModel.Document( 'GTFS', 'venue', model_id )
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
        this.push( doc );
      }
      catch ( ex ){
        stats.badRecordCount++;
      }

      next();
    }
  );
}

module.exports = {
  create: createDocumentStream
};
