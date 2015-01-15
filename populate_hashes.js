var async = require('async');
var db = require('./server/database');
var lib = require('./server/lib');
var _ = require('lodash');

var offset = 1e6;

var game = 1e6; // You might want to make this 10M for a prod setting..
var serverSeed = 'DO NOT USE THIS SEED';

function loop(cb) {
    var parallel = Math.min(game, 1000);

    var inserts = _.range(parallel).map(function() {

        return function(cb) {
            serverSeed = lib.genGameHash(serverSeed);
            game--;

            db.query('INSERT INTO game_hashes(game_id, hash) VALUES($1, $2)', [offset + game, serverSeed], cb);
        };
    });

    async.parallel(inserts, function(err) {
        if (err) throw err;

        if (game > 0)
            loop(cb);
        else
            cb();
    });
}


loop(function() {

    console.log('Finished with serverseed: ', serverSeed);

});