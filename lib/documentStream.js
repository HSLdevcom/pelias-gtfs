var through = require( 'through2' );
var peliasModel = require( 'pelias-model' );
var logger = require( 'pelias-logger' ).get( 'pelias-GTFS' );

function mapTypes(types) {
  if (types.length == 1) {
    mode = types[0];
  // set category for single transport mode stops
    switch(mode) {
    case 'TRAM':
      return 'tram stop';
    case 'SUBWAY':
      return 'subway station';
    case 'BUS':
      return 'bus stop';
    case 'FERRY':
      return 'ferry pier';
    case 'RAIL':
      return 'train stop';
    case 'AIRPLANE':
      return 'airport';
    }
  }
  return null;
}

function testRouteType(types, test) {
  var hit;

  Object.keys(types).forEach(type => {
    var val = Number.parseInt(type);
    if (!Number.isNaN(val) && test(val)) {
      hit = true;
    }
  });
  return hit;
}

function buildTransportModeString(types) {
  if (!types) {
    return null;
  }

  var names = []; // GTFS route_type codes mapped to string

  // consider original GTFS route_types and new extended ones as well
  if (testRouteType(types, val => (val === 3 || (val >= 700 && val < 800)))) {
    names.push('BUS');
  }
  if (testRouteType(types, val => (val === 702))) {
    names.push('BUS-EXPRESS'); // trunk routes
  }
  if (testRouteType(types, val => (val === 704))) {
    names.push('BUS-LOCAL'); // neighborhood routes
  }
  if (testRouteType(types, val => (val === 0 || (val >= 900 && val < 1000)))) {
    names.push('TRAM');
  }
  if (testRouteType(types, val => (val === 2 || (val >= 100 && val < 200)))) {
    names.push('RAIL');
  }
  if (testRouteType(types, val => (val === 1 || (val >= 400 && val < 500)))) {
    names.push('SUBWAY');
  }
  if (testRouteType(types, val => (val >= 1100 && val < 1200))) {
    names.push('AIRPLANE');
  }
  if (testRouteType(types, val => (val === 4 || (val >= 1200 && val < 1300)))) {
    names.push('FERRY');
  }
  if (names.length) {
    return names;
  }
  return null;
}

function dedupePostfix(name, code) {
  var c = String(code).toLowerCase();
  var narr = name.toLowerCase().split(' ');
  return !narr.includes(c);
}

const plat = {
  fi: ' laituri ',
  sv: ' platform ',
  en: ' platform '
};

/*
 * Create a stream of Documents from valid CSV records
 */
function createDocumentStream(translations, prefix, activeStops, stopRouteTypes) {

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
      if (activeStops.notDefined || activeStops[record.stop_id]) {
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
          var fullName = name;
          var type;
          if (record.location_type === '1') {
            type = 'station';
          } else {
            type = 'stop';
          }
          var doc = new peliasModel.Document( sourceName, type, model_id )
              .setName( 'default', fullName )
              .setCentroid( { lon: record.stop_lon, lat: record.stop_lat } );

          if (record.platform_code) {
            doc.setNameAlias( 'default', name + plat.fi + record.platform_code);
          }
          if(name !== fullName) {
            doc.setName( 'alternative', name ); // plain name without code part
          }
          if(shortName) {
            doc.setName('short', shortName);
          }
          if (translations) {
            var names = translations[name];
            if (names) {
              for (var lang in names) {
                var name2 = names[lang];
                if (name2 !== name) {
                  doc.setName(lang, name2);
                  if (record.platform_code) {
                    doc.setNameAlias( lang, name2 + plat[lang] + record.platform_code);
                  }
                }
              }
            }
          }

          var gtfsAdd = {};

          if (record.platform_code) {
            gtfsAdd.platform = record.platform_code;
          }
          var transportModes = buildTransportModeString(stopRouteTypes[record.stop_id]);
          if (transportModes) {
            gtfsAdd.modes = transportModes;
            var vehicleType = mapTypes(transportModes);
            if (vehicleType) {
              doc.addCategory(vehicleType);
            }
          }
          if (transportModes || record.platform_code) {
            doc.setAddendum('GTFS', gtfsAdd);
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
