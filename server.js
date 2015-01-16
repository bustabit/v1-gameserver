var async = require('async');
var assert = require('assert');
var constants = require('constants');
var fs = require('fs');
var path = require('path');

var socket = require('./server/socket');
var database = require('./server/database');
var Game = require('./server/game');
var Chat = require('./server/chat');
var GameHistory = require('./server/game_history');

var _ = require('lodash');

var port = process.env.PORT || 3842;

var server;

if (process.env.USE_HTTPS) {
    var options = {
        key: fs.readFileSync(process.env.HTTPS_KEY || path.join(__dirname, 'key.pem')),
        cert: fs.readFileSync(process.env.HTTPS_CERT || path.join(__dirname, 'cert.pem')),
        secureProtocol: 'SSLv23_method',
        secureOptions: constants.SSL_OP_NO_SSLv3 | constants.SSL_OP_NO_SSLv2
    };

    if (process.env.HTTPS_CA) {
        options.ca = fs.readFileSync(process.env.HTTPS_CA);
    }

    server = require('https').createServer(options).listen(port, function() {
        console.log('Listening on port ', port, ' on HTTPS!');
    });
} else {
    server = require('http').createServer().listen(port, function() {
        console.log('Listening on port ', port, ' with http');
    });
}

async.parallel([
    database.getGameHistory,
    database.getLastGameInfo
], function(err, results) {
    if (err) {
        console.error('[INTERNAL_ERROR] got error: ', err,
            'Unable to get table history');
        throw err;
    }

    var gameHistory = new GameHistory(results[0]);
    var info = results[1];
    var lastGameId = info.id;
    var lastHash = info.hash;
    assert(typeof lastGameId === 'number');

    var game = new Game(lastGameId, lastHash, gameHistory);
    var chat = new Chat();

    socket(server, game, chat);

});
