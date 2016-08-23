var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );

function mapVehicleType(type) {
  switch(type) {
  case '0':
    return ['raitiovaunupysäkki', 'spårvagnsstop', 'tram stop'];
    break;
  case '1':
    return ['metroasema', 'tunnelbanestation', 'subway station'];
    break;
  case '3':
    return ['bussipysäkki', 'busshållplats', 'bus stop'];
    break;
  case '4':
    return ['lauttalaituri', 'färjan kaj', 'ferry pier'];
    break;
  case '109':
    return ['junapysäkki', 'järnvägsstation', 'train stop'];
    break;
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
            for (lang in names) {
              name2 = names[lang];
              if (name2 !== name) {
                doc.setName( lang, name2 + stopCode );
              }
            }
          }
        }
        var vehicleTypes = mapVehicleType(record.vehicle_type);
        if (vehicleTypes) {
          for (type in vehicleTypes) {
            doc.addCategory(type);
          }
        }
        this.push( doc );
      }
      catch ( ex ){
        logger.info('bad record', record);
        stats.badRecordCount++;
      }

      next();
    }
  );
}

module.exports = {
  create: createDocumentStream
};
