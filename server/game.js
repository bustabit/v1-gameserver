var assert = require('better-assert');
var async = require('async');
var db = require('./database');
var events = require('events');
var util = require('util');
var _ = require('lodash');
var lib = require('./lib');
var SortedArray = require('./sorted_array');
var config = require('./config');

var tickRate = 150; // ping the client every X miliseconds
var afterCrashTime = 3000; // how long from game_crash -> game_starting
var restartTime = 5000; // How long from  game_starting -> game_started

function Game(lastGameId, lastHash, bankroll, gameHistory) {
    var self = this;

    self.bankroll = bankroll;
    self.maxWin = 0;

    self.controllerIsRunning = false; // if we are running games. there can still be a game in progress
    self.startTime; // time game started. If before game started, is an estimate...
    self.crashPoint; // when the game crashes, 0 means instant crash
    self.gameDuration; // how long till the game will crash..

    self.openBet = 0; // how much satoshis is still in action
    self.totalWon = 0; // how much satoshis players won (profit)
    self.forcePoint = null; // The point we force terminate the game

    self.state = 'ENDED'; // 'STARTING' | 'BLOCKING' | 'IN_PROGRESS' |  'ENDED'
    self.pending = {}; // Set of players pending a joined
    self.pendingCount = 0;
    self.joined = new SortedArray(); // A list of joins, before the game is in progress
    self.players = {}; // An object of userName ->  { playId: ..., autoCashOut: .... }

    // An array that approximates playing users, i.e. the ones that have not yet
    // cashed out, for O(1) auto cashouts. Plays are inserted by increasing
    // autoCashOut order during game start and only shifted durig the game ticks
    // up to the current multiplier. This means, at any point it contains only
    // the players that have a higher cashout than the currenty multiplier and
    // all other have been cashed out.
    self.playing = [];
    self.gameId = lastGameId;
    self.gameHistory = gameHistory;

    self.lastHash = lastHash;
    self.hash = null;

    events.EventEmitter.call(self);

    function runGame() {

        db.createGame(self.gameId + 1, function (err, info) {
            if (err) {
                console.log('Could not create game', err, ' retrying in 2 sec..');
                setTimeout(runGame, 2000);
                return;
            }



            self.state = 'STARTING';
            self.crashPoint = info.crashPoint;
            self.openBet = 0;
            self.totalWon = 0;

            if (config.CRASH_AT) {
                assert(!config.PRODUCTION);
                self.crashPoint = parseInt(config.CRASH_AT);
            }

            self.hash = info.hash;
            self.gameId++;
            self.startTime = new Date(Date.now() + restartTime);
            self.players = {}; // An object of userName ->  { user: ..., playId: ..., autoCashOut: ...., status: ... }
            self.playing = [];
            self.gameDuration = Math.ceil(inverseGrowth(self.crashPoint + 1)); // how long till the game will crash..
            self.maxWin = Math.round(self.bankroll * 0.03); // Risk 3% per game

            self.emit('game_starting', {
                game_id: self.gameId,
                max_win: self.maxWin,
                time_till_start: restartTime
            });

            setTimeout(blockGame, restartTime);
        });
    }

    function blockGame() {
        self.state = 'BLOCKING'; // we're waiting for pending bets..

        loop();
        function loop() {
            if (self.pendingCount > 0) {
                console.log('Delaying game by 100ms for ', self.pendingCount , ' joins');
                return setTimeout(loop, 100);
            }
            startGame();
        }
    }

    function startGame() {
        self.state = 'IN_PROGRESS';
        self.startTime = new Date();
        self.pending = {};
        self.pendingCount = 0;

        var bets = {};
        var arr = self.playing = self.joined.getArray();
        for (var i = 0; i < arr.length; ++i) {
            var a = arr[i];
            bets[a.user.username] = a.bet;
            self.players[a.user.username] = a;
        }

        self.joined.clear();
        self.playing.sort(function(a,b) {
          return a.autoCashOut - b.autoCashOut;
        });

        self.emit('game_started', bets);

        self.setForcePoint();

        callTick(0);
    }

    function callTick(elapsed) {
        var left = self.gameDuration - elapsed;
        var nextTick = Math.max(0, Math.min(left, tickRate));

        setTimeout(runTick, nextTick);
    }


    function runTick() {

        var elapsed = new Date() - self.startTime;
        var at = growthFunc(elapsed);

        self.runCashOuts(at);

        if (self.forcePoint <= at && self.forcePoint <= self.crashPoint) {
            self.cashOutAll(self.forcePoint, function (err) {
                console.log('Just forced cashed out everyone at: ', self.forcePoint, ' got err: ', err);

                endGame(true);
            });
            return;
        }

        // and run the next

        if (at > self.crashPoint)
            endGame(false); // oh noes, we crashed!
        else
            tick(elapsed);
    }

    function endGame(forced) {
        var gameId = self.gameId;
        var crashTime = Date.now();

        assert(self.crashPoint == 0 || self.crashPoint >= 100);

        var bonuses = [];

        if (self.crashPoint !== 0) {
            bonuses = calcBonuses(self.players);

            var givenOut = 0;
            Object.keys(self.players).forEach(function(player) {
                var record = self.players[player];

                givenOut += record.bet * 0.01;
                if (record.status === 'CASHED_OUT') {
                    var given = record.stoppedAt * (record.bet / 100);
                    assert(lib.isInt(given) && given > 0);
                    givenOut += given;
                }
            });

            self.bankroll -= givenOut;
        }

        var playerInfo = self.getInfo().player_info;
        var bonusJson = {};
        bonuses.forEach(function(entry) {
            bonusJson[entry.user.username] = entry.amount;
            playerInfo[entry.user.username].bonus = entry.amount;
        });

        self.lastHash = self.hash;

        // oh noes, we crashed!
        self.emit('game_crash', {
            forced: forced,
            elapsed: self.gameDuration,
            game_crash: self.crashPoint, // We send 0 to client in instant crash
            bonuses: bonusJson,
            hash: self.lastHash
        });

        self.gameHistory.addCompletedGame({
            game_id: gameId,
            game_crash: self.crashPoint,
            created: self.startTime,
            player_info: playerInfo,
            hash: self.lastHash
        });

        var dbTimer;
        dbTimeout();
        function dbTimeout() {
            dbTimer = setTimeout(function() {
                console.log('Game', gameId, 'is still ending... Time since crash:',
                            ((Date.now() - crashTime)/1000).toFixed(3) + 's');
                dbTimeout();
            }, 1000);
        }

        db.endGame(gameId, bonuses, function(err) {
            if (err)
                console.log('ERROR could not end game id: ', gameId, ' got err: ', err);
            clearTimeout(dbTimer);
            scheduleNextGame(crashTime);
        });

        self.state = 'ENDED';
    }

    function scheduleNextGame(crashTime) {
        if (self.controllerIsRunning)
            setTimeout(runGame, (crashTime + afterCrashTime) - Date.now());
        else
            console.log('Game paused');
    }

    // Hack: Assign this method here instead of putting it in the prototype,
    // because it needs runGame() and it also has to be accessible from sockets.
    self.resume = function() {
        // Check if its safe to run a new game.
        if (self.controllerIsRunning || self.state !== 'ENDED')
            return console.log('Game still active');

        console.log('Game resuming');
        self.controllerIsRunning = true;
        runGame();
    };

    // Hack: Just keeping together what belongs together.
    self.pause = function() {
        console.warn('Game is going to pause');
        self.controllerIsRunning = false;
    };

    function tick(elapsed) {
        self.emit('game_tick', elapsed);
        callTick(elapsed);
    }
}

util.inherits(Game, events.EventEmitter);

Game.prototype.getInfo = function() {

    var playerInfo = {};

    for (var username in this.players) {
        var record = this.players[username];

        assert(lib.isInt(record.bet));
        var info = {
            bet: record.bet
        };

        if (record.status === 'CASHED_OUT') {
            assert(lib.isInt(record.stoppedAt));
            info['stopped_at'] = record.stoppedAt;
        }

        playerInfo[username] = info;
    }


    var res = {
        state: this.state,
        player_info: playerInfo,
        game_id: this.gameId, // game_id of current game, if game hasnt' started its the last game
        last_hash: this.lastHash,
        max_win: this.maxWin,
        // if the game is pending, elapsed is how long till it starts
        // if the game is running, elapsed is how long its running for
        /// if the game is ended, elapsed is how long since the game started
        elapsed: Date.now() - this.startTime,
        created: this.startTime,
        joined: this.joined.getArray().map(function(u) { return u.user.username; })
    };

    if (this.state === 'ENDED')
        res.crashed_at = this.crashPoint;

    return res;
};

// Calls callback with (err, booleanIfAbleToJoin)
Game.prototype.placeBet = function(user, betAmount, autoCashOut, callback) {
    var self = this;

    assert(typeof user.id === 'number');
    assert(typeof user.username === 'string');
    assert(lib.isInt(betAmount));
    assert(lib.isInt(autoCashOut) && autoCashOut >= 100);

    if (self.state !== 'STARTING')
        return callback('GAME_IN_PROGRESS');

    if (lib.hasOwnProperty(self.pending, user.username) || lib.hasOwnProperty(self.players, user.username))
        return callback('ALREADY_PLACED_BET');

    self.pending[user.username] = user.username;
    self.pendingCount++;

    db.placeBet(betAmount, autoCashOut, user.id, self.gameId, function(err, playId) {
        self.pendingCount--;

        if (err) {
            if (err.code == '23514' || err.sqlState == '23514') // constraint violation
                return callback('NOT_ENOUGH_MONEY');

            console.log('[INTERNAL_ERROR] could not play game, got error: ', err);
            callback(err);
        } else {
            assert(playId > 0);

            self.bankroll += betAmount;
            self.openBet += betAmount;

            var index = self.joined.insert({ user: user, bet: betAmount, autoCashOut: autoCashOut, playId: playId, status: 'PLAYING' });

            self.emit('player_bet',  {
                username: user.username,
                index: index
            });

            callback(null);
        }
    });
};


Game.prototype.doCashOut = function(play, at, callback) {
    assert(typeof play.user.username === 'string');
    assert(typeof play.user.id == 'number');
    assert(typeof play.playId == 'number');
    assert(typeof at === 'number');
    assert(typeof callback === 'function');

    var self = this;
    var username = play.user.username;

    assert(play === self.players[username]);
    assert(play.status === 'PLAYING');
    play.status = 'CASHED_OUT';
    play.stoppedAt = at;

    var cashed = play.bet * at / 100;
    var won    = play.bet * (at - 100) / 100;  // as in profit
    assert(lib.isInt(cashed));
    assert(lib.isInt(won));

    self.emit('cashed_out', {
        username: username,
        stopped_at: at
    });

    self.openBet  -= play.bet;
    self.totalWon += won;

    db.cashOut(play.user.id, play.playId, cashed, function(err) {
        if (err) {
            console.log('[INTERNAL_ERROR] could not cash out: ', username, ' at ', at, ' in ', play, ' because: ', err);
            return callback(err);
        }

        callback(null);
    });
};

Game.prototype.runCashOuts = function(at) {
    var self = this;
    var update = false; // Check for auto cashouts

    dropWhile(self.playing, function(play) {
        // Strip cashed players from the array
        if (play.status === 'CASHED_OUT')
            return true;

        assert(play.status === 'PLAYING');
        assert(play.autoCashOut);

        if (play.autoCashOut <= at && play.autoCashOut <= self.crashPoint && play.autoCashOut <= self.forcePoint) {
            self.doCashOut(play, play.autoCashOut, function (err) {
                if (err)
                    console.log('[INTERNAL_ERROR] could not auto cashout ', play.username, ' at ', play.autoCashOut);
            });
            update = true;
            return true; // Drop from self.playing
        } else {
            return false; // Don't drop this one and stop dropping here
        }
    });

    if (update)
        self.setForcePoint();
};

Game.prototype.setForcePoint = function() {
   var self = this;

   if (!config.production) {
       var openBet = 0; // how much satoshis is still in action
       var totalWon = 0; // how much satoshis has been lost

       Object.keys(self.players).forEach(function(playerName) {
           var play = self.players[playerName];

           if (play.status === 'CASHED_OUT') {
               var amount = play.bet * (play.stoppedAt - 100) / 100;
               totalWon += amount;
           } else {
               assert(play.status == 'PLAYING');
               assert(lib.isInt(play.bet));
               openBet += play.bet;
           }
       });

       assert(self.openBet === openBet);
       assert(self.totalWon === totalWon);
   }

   if (self.openBet === 0) {
       self.forcePoint = Infinity; // the game can go until it crashes, there's no end.
   } else {
       // TODO: Subtract the bonus of all bets should instead of just the open bets.
       var left = self.maxWin - self.totalWon - (self.openBet * 0.01);
       var ratio = (left+self.openBet) / self.openBet;

       // in percent
       self.forcePoint = Math.max(Math.floor(ratio * 100), 101);
   }
};

Game.prototype.cashOut = function(user, callback) {
    var self = this;

    assert(typeof user.id === 'number');

    if (this.state !== 'IN_PROGRESS')
        return callback('GAME_NOT_IN_PROGRESS');

    var elapsed = new Date() - self.startTime;
    var at = growthFunc(elapsed);
    var play = lib.getOwnProperty(self.players, user.username);

    if (!play)
        return callback('NO_BET_PLACED');

    if (play.autoCashOut <= at)
        at = play.autoCashOut;

    if (self.forcePoint <= at)
        at = self.forcePoint;


    if (at > self.crashPoint)
        return callback('GAME_ALREADY_CRASHED');

    if (play.status === 'CASHED_OUT')
        return callback('ALREADY_CASHED_OUT');

    self.doCashOut(play, at, callback);
    self.setForcePoint();
};

Game.prototype.cashOutAll = function(at, callback) {
    var self = this;

    if (this.state !== 'IN_PROGRESS')
        return callback();

    console.log('Cashing everyone out at: ', at);

    assert(at >= 100);

    self.runCashOuts(at);

    if (at > self.crashPoint)
        return callback(); // game already crashed, sorry guys

    var tasks = [];

    Object.keys(self.players).forEach(function(playerName) {
        var play = self.players[playerName];

        if (play.status === 'PLAYING') {
            tasks.push(function (callback) {
                if (play.status === 'PLAYING')
                    self.doCashOut(play, at, callback);
                else
                    callback();
            });
        }
    });

    console.log('Needing to force cash out: ', tasks.length, ' players');

    async.parallelLimit(tasks, 4, function (err) {
        if (err) {
            console.error('[INTERNAL_ERROR] unable to cash out all players in ', self.gameId, ' at ', at);
            callback(err);
            return;
        }
        console.log('Emergency cashed out all players in gameId: ', self.gameId);

        callback();
    });
};

/// returns [ {playId: ?, user: ?, amount: ? }, ...]
function calcBonuses(input) {
    // first, lets sum the bets..

    function sortCashOuts(input) {
        function r(c) {
            return c.stoppedAt ? -c.stoppedAt : null;
        }

        return _.sortBy(input, r);
    }

    // slides fn across array, providing [listRecords, stoppedAt, totalBetAmount]
    function slideSameStoppedAt(arr, fn) {
        var i = 0;
        while (i < arr.length) {
            var tmp = [];
            var betAmount = 0;
            var sa = arr[i].stoppedAt;
            for (; i < arr.length && arr[i].stoppedAt === sa; ++i) {
                betAmount += arr[i].bet;
                tmp.push(arr[i]);
            }
            assert(tmp.length >= 1);
            fn(tmp, sa, betAmount);
        }
    }

    var results = [];

    var sorted = sortCashOuts(input);

    if (sorted.length  === 0)
        return results;

    var bonusPool = 0;
    var largestBet = 0;

    for (var i = 0; i < sorted.length; ++i) {
        var record = sorted[i];

        assert(record.status === 'CASHED_OUT' || record.status === 'PLAYING');
        assert(record.playId);
        var bet = record.bet;
        assert(lib.isInt(bet));

        bonusPool += bet / 100;
        assert(lib.isInt(bonusPool));

        largestBet = Math.max(largestBet, bet);
    }

    var maxWinRatio = bonusPool / largestBet;

    slideSameStoppedAt(sorted,
        function(listOfRecords, cashOutAmount, totalBetAmount) {
            if (bonusPool <= 0)
                return;

            var toAllocAll = Math.min(totalBetAmount * maxWinRatio, bonusPool);

            for (var i = 0; i < listOfRecords.length; ++i) {
                var toAlloc = Math.round((listOfRecords[i].bet / totalBetAmount) * toAllocAll);

                if (toAlloc <= 0)
                    continue;

                bonusPool -= toAlloc;

                var playId = listOfRecords[i].playId;
                assert(lib.isInt(playId));
                var user = listOfRecords[i].user;
                assert(user);

                results.push({
                    playId: playId,
                    user: user,
                    amount: toAlloc
                });
            }
        }
    );

    return results;
}


function growthFunc(ms) {
    var r = 0.00006;
    return Math.floor(100 * Math.pow(Math.E, r * ms));
}

function inverseGrowth(result) {
    var c = 16666.666667;
    return c * Math.log(0.01 * result);
}

function dropWhile(arr, pred) {
  for (var l = arr.length; l > 0 && pred(arr[0]); --l, arr.shift()) ;
}

module.exports = Game;
