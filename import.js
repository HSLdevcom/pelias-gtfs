/**
 * @file entry for the gtfs stop import pipeline
 */

var importPipeline = require( './lib/importPipeline' );
var argv = require('minimist')(process.argv.slice(2)); // -d datapath/ --prefix=xxx

importPipeline.create( argv.d, argv.prefix );
