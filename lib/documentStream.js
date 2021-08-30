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

function dedupePostfix(name, code) {
  var c = String(code).toLowerCase();
  var narr = name.toLowerCase().split(' ');
  return !narr.includes(c);
}

/*
 * Create a stream of Documents from valid CSV records
 */
function createDocumentStream(translations, prefix, activeStops) {

  var badRecordCount=0;
  var unusedStopCount=0;
  var sourceName = 'GTFS';
  var fullPrefix = sourceName + ':';

  if(prefix) {
    fullPrefix += prefix + ':'; // e.g. 'GTFS:HSL:'
    sourceName += prefix;
  }
  return through.obj(
    function write( record, enc, next ){
      if (!activeStops || activeStops[record.stop_id]) {
	try {
          var model_id = fullPrefix + record.stop_id;
          var name = record.stop_name;
          var stopCode = '';
          var shortName;
          if (record.stop_code && record.stop_code !== '') {
            if (dedupePostfix(name, record.stop_code)) {
              // append to full name if not already appended
              stopCode = ' ' + record.stop_code;
	      model_id += '#' + record.stop_code;
              shortName = record.stop_code;
            }
          }
          var fullName = name + stopCode;
          var type;
          if (record.location_type === '1') {
            type = 'station';
          } else {
            type = 'stop';
          }
          var doc = new peliasModel.Document( sourceName, type, model_id )
              .setName( 'default', fullName )
              .setName( 'fi', fullName )
              .setCentroid( { lon: record.stop_lon, lat: record.stop_lat } );

          if(shortName) {
            doc.setName( 'short', shortName );
          }

          if(name !== fullName) {
            doc.setName( 'alternative', name ); // plain name without code part
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
            // stops are not wanted, set a low popularity
            doc.setPopularity(5);
          }

          this.push( doc );
	}
	catch ( ex ){
          badRecordCount++;
	}
      } else {
	unusedStopCount++;
	// logger.info('Skipping unused stop ' + record.stop_name);
      }
      next();
    }, function end( done ) {
      logger.info('Bad record count: ' + badRecordCount);
      logger.info('Filtered unused stops: ' + unusedStopCount);
      done();
    }
  );
}

module.exports = {
  create: createDocumentStream
};
