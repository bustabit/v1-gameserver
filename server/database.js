var assert = require('assert');
var uuid = require('uuid');

var async = require('async');
var lib = require('./lib');
var pg = require('pg');

var databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl)
    throw new Error('must set DATABASE_URL environment var');

console.log('DATABASE_URL: ', databaseUrl);

// Increase the client pool size. At the moment the most concurrent
// queries are performed when auto-bettors join a newly created
// game. (A game is ended in a single transaction). With an average
// of 25-35 players per game, an increase to 20 seems reasonable to
// ensure that most queries are submitted after around 1 round-trip
// waiting time or less.
pg.defaults.poolSize = 20;

// The default timeout is 30s, or the time from 1.00x to 6.04x.
// Considering that most of the action happens during the beginning
// of the game, this causes most clients to disconnect every ~7-9
// games only to be reconnected when lots of bets come in again during
// the next game. Bump the timeout to 2 min (or 1339.43x) to smooth
// this out.
pg.defaults.poolIdleTimeout = 120000;

pg.types.setTypeParser(20, function(val) { // parse int8 as an integer
    return val === null ? null : parseInt(val);
});

pg.types.setTypeParser(1700, function(val) { // parse numeric as a float
    return val === null ? null : parseFloat(val);
});

// callback is called with (err, client, done)
function connect(callback) {
    return pg.connect(databaseUrl, callback);
}

function query(query, params, callback) {
    //third parameter is optional
    if (typeof params == 'function') {
        callback = params;
        params = [];
    }

    doIt();
    function doIt() {
        connect(function(err, client, done) {
            if (err) return callback(err);
            client.query(query, params, function(err, result) {
                done();
                if (err) {
                    if (err.code === '40P01') {
                        console.log('Warning: Retrying deadlocked transaction: ', query, params);
                        return doIt();
                    }
                    return callback(err);
                }

                callback(null, result);
            });
        });
    }
}

function getClient(runner, callback) {
    doIt();

    function doIt() {
        connect(function (err, client, done) {
            if (err) return callback(err);

            function rollback(err) {
                client.query('ROLLBACK', done);

                if (err.code === '40P01') {
                    console.log('Warning: Retrying deadlocked transaction..');
                    return doIt();
                }

                callback(err);
            }

            client.query('BEGIN', function (err) {
                if (err)
                    return rollback(err);

                runner(client, function (err, data) {
                    if (err)
                        return rollback(err);

                    client.query('COMMIT', function (err) {
                        if (err)
                            return rollback(err);

                        done();
                        callback(null, data);
                    });
                });
            });
        });
    }
}


exports.query = query;

// runner takes (client, callback)

// callback should be called with (err, data)
// client should not be used to commit, rollback or start a new transaction

// callback takes (err, data)

exports.getLastGameInfo = function(callback) {
    query('SELECT MAX(id) id FROM games', function(err, results) {
        if (err) return callback(err);
        assert(results.rows.length === 1);

        var id = results.rows[0].id;

        if (!id || id < 1e6) {
            return callback(null, {
                id: 1e6 - 1,
                hash: 'c1cfa8e28fc38999eaa888487e443bad50a65e0b710f649affa6718cfbfada4d'
            });
        }

        query('SELECT hash FROM game_hashes WHERE game_id = $1', [id], function(err, results) {
            if (err) return callback(err);

            assert(results.rows.length === 1);

            callback(null, {
                id: id,
                hash: results.rows[0].hash
            });
        });
    });
};

exports.getUserByName = function(username, callback) {
    assert(username);
    query('SELECT * FROM users WHERE lower(username) = lower($1)', [username], function(err, result) {
        if (err) return callback(err);
        if (result.rows.length === 0)
            return callback('USER_DOES_NOT_EXIST');

        assert(result.rows.length === 1);
        callback(null, result.rows[0]);
    });
};

exports.getUserAndGamesPlayedByName = function(username, callback) {
    assert(username);
    query('SELECT *, (SELECT COUNT(*) AS games_played FROM plays WHERE user_id = users.id) ' +
          'FROM users WHERE lower(username) = lower($1)', [username], function(err, result) {
        if (err) return callback(err);
        if (result.rows.length === 0)
            return callback('USER_DOES_NOT_EXIST');

        assert(result.rows.length === 1);
        callback(null, result.rows[0]);
    });
};



exports.getUserById = function(userId, callback) {
    assert(userId);
    query('SELECT * FROM users WHERE id = $1', [userId], function(err, result) {
        if (err) return callback(err);
        if (result.rows.length === 0)
            return callback('USER_DOES_NOT_EXIST');

        assert(result.rows.length === 1);
        callback(null, result.rows[0]);
    });
};

exports.updateEmail = function(userId, email, callback) {
    assert(userId);

    query('UPDATE users SET email = $1 WHERE id = $2', [email, userId], function(err, res) {
        if(err) return callback(err);

        assert(res.rowCount === 1);
        callback(null);
    });

};



exports.deleteUserSession = function(sessionId, callback) {
    assert(sessionId);
    query('DELETE FROM sessions WHERE user_id = (SELECT user_id FROM sessions WHERE id = $1)', [sessionId], callback);
};

function createSession(client, userId, callback) {
    var sessionId = uuid.v4();

    client.query('INSERT INTO sessions(id, user_id) VALUES($1, $2) RETURNING id', [sessionId, userId], function(err, res) {
        if (err) return callback(err);
        assert(res.rows.length === 1);

        var session = res.rows[0];
        assert(session.id);

        callback(null, session.id);
    });
};

exports.createOneTimeToken = function(userId, callback) {
    assert(userId);
    var id = uuid.v4();

    query('INSERT INTO sessions(id, user_id, ott) VALUES($1, $2, true) RETURNING id', [id, userId], function(err, result) {
        if (err) return callback(err);
        assert(result.rows.length === 1);

        var ott = result.rows[0];

        callback(null, ott.id);
    });
};

exports.validateOneTimeToken = function(token, callback) {
    assert(token);

    query('WITH t as (DELETE FROM sessions WHERE id = $1 AND ott = TRUE RETURNING *) ' +
        'SELECT * FROM users WHERE id = (SELECT user_id FROM t)',
        [token], function(err, result) {
            if (err) return callback(err);
            if (result.rowCount == 0) return callback('NOT_VALID_TOKEN');
            assert(result.rows.length === 1);
            callback(null, result.rows[0]);
        }
    );
};

exports.createSession = function(userId, callback) {
    assert(userId && callback);

    getClient(function(client, callback) {
        createSession(client, userId, callback);
    }, callback);

};

exports.getUserFromUsername = function(username, callback) {
    assert(username && callback);

    query('SELECT * FROM users_view WHERE lower(username) = lower($1)', [username], function(err, data) {
        if (err) return callback(new Error('Unable to query get user by username: ' + username + '\n' + err));

        if (data.rows.length === 0)
            return callback('NO_USER');

        assert(data.rows.length === 1);
        var user = data.rows[0];
        assert(typeof user.balance_satoshis === 'number');

        callback(null, user);
    });
};

exports.addRecoverId = function(userId, callback) {
    assert(userId && callback);

    var recoveryId = uuid.v4();

    query('INSERT INTO recovery (id, user_id)  values($1, $2)', [recoveryId, userId], function(err, res) {
        if (err) return callback(new Error('Unable to insert recovery id: ' + recoveryId + ' for user ' + userId + '\n' + err));
        callback(null, recoveryId);
    });
};

exports.getUserBySessionId = function(sessionId, callback) {
    assert(sessionId && callback);
    query('SELECT * FROM users_view WHERE id = (SELECT user_id FROM sessions WHERE id = $1 AND ott = FALSE)', [sessionId], function(err, response) {
        if (err) return callback(new Error('Unable to query user by session id ' + sessionId + '\n' + err));

        var data = response.rows;
        if (data.length === 0)
            return callback('NOT_VALID_SESSION');

        assert(data.length === 1);

        var user = data[0];
        assert(typeof user.balance_satoshis === 'number');

        callback(null, user);
    });
};

exports.getUserByRecoverId = function(recoverId, callback) {
    assert(recoverId && callback);
    query('SELECT * FROM users_view WHERE id = (SELECT user_id FROM recovery WHERE id = $1)', [recoverId], function(err, res) {
        if (err) return callback(new Error('Unable to get user by recover id :' + recoverId + '\n' + err));

        var data = res.rows;
        if (data.length === 0)
            return callback('NOT_VALID_RECOVER_ID');

        assert(data.length === 1);
        return callback(null, data[0]);
    });
};


exports.placeBet = function(amount, autoCashOut, userId, gameId, callback) {
    assert(typeof amount === 'number');
    assert(typeof autoCashOut === 'number');
    assert(typeof userId === 'number');
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    getClient(function(client, callback) {
      var tasks = [
        function(callback) {
          client.query('UPDATE users SET balance_satoshis = balance_satoshis - $1 WHERE id = $2',
            [amount, userId], callback);
        },
        function(callback) {
          client.query(
            'INSERT INTO plays(user_id, game_id, bet, auto_cash_out) VALUES($1, $2, $3, $4) RETURNING id',
            [userId, gameId, amount, autoCashOut], callback);
        }
      ];

      async.parallel(tasks, function(err, result) {
        if (err)
            return callback(err);

        var playId = result[1].rows[0].id;
        assert(typeof playId === 'number');

        callback(null, playId);
      });
    }, callback);
};


var endGameQuery =
  'WITH vals AS ( ' +
  ' SELECT ' +
  ' unnest($1::bigint[]) as user_id, ' +
  ' unnest($2::bigint[]) as play_id, ' +
  ' unnest($3::bigint[]) as bonus ' +
  '), p AS (' +
  ' UPDATE plays SET bonus = vals.bonus FROM vals WHERE id = vals.play_id RETURNING vals.user_id '+
  '), u AS (' +
  ' UPDATE users SET balance_satoshis = balance_satoshis + vals.bonus ' +
  ' FROM vals WHERE id = vals.user_id RETURNING vals.user_id ' +
  ') SELECT COUNT(*) count FROM p JOIN u ON p.user_id = u.user_id';

exports.endGame = function(gameId, bonuses, callback) {
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');


    getClient(function(client, callback) {
      client.query('UPDATE games SET ended = true WHERE id = $1', [gameId],
        function (err) {
          if (err) return callback(new Error('Could not end game, got: ' + err));


          var userIds = [];
          var playIds = [];
          var bonusesAmounts = [];

          bonuses.forEach(function (bonus) {
            assert(lib.isInt(bonus.user.id));
            userIds.push(bonus.user.id);
            assert(lib.isInt(bonus.playId));
            playIds.push(bonus.playId);
            assert(lib.isInt(bonus.amount) && bonus.amount > 0);
            bonusesAmounts.push(bonus.amount);
          });

          assert(userIds.length == playIds.length && playIds.length == bonusesAmounts.length);

          if (userIds.length === 0)
            return callback();
          
          client.query(endGameQuery, [userIds, playIds, bonusesAmounts], function(err, result) {
            if (err)
              return callback(err);

            if (result.rows[0].count !== userIds.length) {
              throw new Error('Mismatch row count: ' + result.rows[0].count + ' and ' + userIds.length);
            }

            callback();
          });

        });
    }, callback);

};

exports.getGame = function(gameId, callback) {
    assert(gameId);

    query('SELECT * FROM games WHERE id = $1 AND ended = TRUE', [gameId], function(err, result) {
        if (err) return callback(err);
        if (result.rows.length == 0) return callback('GAME_DOES_NOT_EXISTS');
        assert(result.rows.length == 1);
        callback(null, result.rows[0]);
    });
};

exports.getGamesPlays = function(gameId, callback) {
    query('SELECT u.username, p.bet, p.cash_out, p.bonus FROM plays p, users u ' +
        ' WHERE game_id = $1 AND p.user_id = u.id ORDER by p.cash_out DESC NULLS LAST', [gameId],
        function(err, result) {
            if (err) return callback(err);
            return callback(null, result.rows);
        }
    );
};


function addSatoshis(client, userId, amount, callback) {

    client.query('UPDATE users SET balance_satoshis = balance_satoshis + $1 WHERE id = $2', [amount, userId], function(err, res) {
        if (err) return callback(err);
        assert(res.rowCount === 1);
        callback(null);
    });
}


exports.cashOut = function(userId, playId, amount, callback) {
    assert(typeof userId === 'number');
    assert(typeof playId === 'number');
    assert(typeof amount === 'number');
    assert(typeof callback === 'function');

    getClient(function(client, callback) {
        addSatoshis(client, userId, amount, function(err) {
            if (err)
                return callback(err);

            client.query(
                'UPDATE plays SET cash_out = $1 WHERE id = $2 AND cash_out IS NULL',
                [amount, playId], function(err, result) {
                    if (err)
                        return callback(err);

                    if (result.rowCount !== 1) {
                        console.error('[INTERNAL_ERROR] Double cashout? ',
                            'User: ', userId, ' play: ', playId, ' amount: ', amount,
                            ' got: ', result.rowCount);

                        return callback(new Error('Double cashout'));
                    }

                    callback(null);
                }
            );
        });
    }, callback);
};

exports.getUserPlays = function(userId, limit, offset, callback) {
    assert(userId);

    query('SELECT p.bet, p.bonus, p.cash_out, p.created, p.game_id, g.game_crash FROM plays p ' +
        'LEFT JOIN (SELECT * FROM games WHERE ended = true) g ON g.id = p.game_id ' +
        'WHERE p.user_id = $1 ORDER BY p.id DESC LIMIT $2 OFFSET $3',
        [userId, limit, offset], function(err, result) {
            if (err) return callback(err);
            callback(null, result.rows);
        }
    );
};

exports.getGiveAwaysAmount = function(userId, callback) {
    assert(userId);
    query('SELECT SUM(g.amount) FROM giveaways g where user_id = $1', [userId], function(err,result) {
        if (err) return callback(err);
        return callback(null, result.rows[0]);
    });
};

exports.addGiveaway = function(userId, callback) {
    assert(userId);
    getClient(function(client, callback) {

            client.query('SELECT last_giveaway FROM users_view WHERE id = $1', [userId] , function(err, result) {
                if (err) return callback(err);

                if (!result.rows) return callback('USER_DOES_NOT_EXIST');
                assert(result.rows.length === 1);
                var lastGiveaway = result.rows[0].last_giveaway;
                var eligible = lib.isEligibleForGiveAway(lastGiveaway);

                if (typeof eligible === 'number') {
                    return callback({ message: 'NOT_ELIGIBLE', time: eligible});
                }

                var amount = 200; // 2 bits
                client.query('INSERT INTO giveaways(user_id, amount) VALUES($1, $2) ', [userId, amount], function(err) {
                    if (err) return callback(err);

                    addSatoshis(client, userId, amount, function(err) {
                        if (err) return callback(err);

                        callback(null);
                    });
                });
            });

        }, callback
    );
};

exports.addRawGiveaway = function(userNames, amount, callback) {
    assert(userNames && amount);

    getClient(function(client, callback) {

        var tasks = userNames.map(function(username) {
            return function(callback) {

                client.query('SELECT id FROM users WHERE lower(username) = lower($1)', [username], function(err, result) {
                    if (err) return callback('unable to add bits');

                    if (result.rows.length === 0) return callback(new Error(username + ' didnt exists'));

                    var userId = result.rows[0].id;
                    client.query('INSERT INTO giveaways(user_id, amount) VALUES($1, $2) ', [userId, amount], function(err, result) {
                        if (err) return callback(err);

                        assert(result.rowCount == 1);
                        addSatoshis(client, userId, amount, function(err) {
                            if (err) return callback(err);
                            callback(null);
                        });
                    });
                });
            };
        });

        async.series(tasks, function(err, ret) {
            if (err) return callback(err);
            return callback(null, ret);
        });

    }, callback);
};

exports.getUserNetProfit = function(userId, callback) {
    assert(userId);
    query('SELECT (' +
            'COALESCE(SUM(cash_out), 0) + ' +
            'COALESCE(SUM(bonus), 0) - ' +
            'COALESCE(SUM(bet), 0)) profit ' +
        'FROM plays ' +
        'WHERE user_id = $1', [userId], function(err, result) {
            if (err) return callback(err);
            assert(result.rows.length == 1);
            return callback(null, result.rows[0]);
        }
    );
};

exports.getUserNetProfitSkip = function(userId, skip, callback) {
    assert(userId);
    query('SELECT (' +
            'COALESCE(SUM(cash_out), 0) + ' +
            'COALESCE(SUM(bonus), 0) - ' +
            'COALESCE(SUM(bet), 0))::bigint profit ' +
            'FROM ( ' +
                'SELECT * FROM plays ' +
                'WHERE user_id = $1 ' +
                'ORDER BY id DESC ' +
                'OFFSET $2 ' +
            ') restricted ', [userId, skip], function(err, result) {
            if (err) return callback(err);
            assert(result.rows.length == 1);
            return callback(null, result.rows[0].profit);
        }
    );
};

exports.getPublicStats = function(username, callback) {

    query('SELECT * FROM leaderboard WHERE lower(username) = lower($1)',
        [username], function(err, result) {
            if (err) return callback(err);

            if (result.rows.length !== 1)
                return callback('USER_DOES_NOT_EXIST');

            return callback(null, result.rows[0]);
        }
    );
};




exports.getUserChartData = function(userId, callback) {
    assert(typeof userId === 'number');
    assert(typeof callback === 'function');

    query('WITH raw AS ( ' +
            'SELECT ' +
            '(coalesce(cash_out, 0) + coalesce(bonus, 0) - bet) profit, ' +
            'row_number() over (ORDER BY id) as rnum ' +
            'FROM plays ' +
            'WHERE user_id = $1 ' +
    ') ' +
    'SELECT SUM(profit) profit FROM raw ' +
    'GROUP BY (rnum-1) / (SELECT GREATEST(COUNT(*) / 200.0, 1)::int res FROM raw) ' +
    'ORDER BY (rnum-1) / (SELECT GREATEST(COUNT(*) / 200.0, 1)::int res FROM raw)',
        [userId], function(err, result) {

        if (err)
            return callback(err);

        var running = 0;

        var transformed = result.rows.map(function(row) {
            running += row.profit;
            return running;
        });

        callback(null, transformed);
    });
};

// callback called with (err, { crashPoint: , hash: })
exports.createGame = function(gameId, callback) {
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    query('SELECT hash FROM game_hashes WHERE game_id = $1', [gameId], function(err, results) {
        if (err) return callback(err);

        if (results.rows.length !==  1) {
            console.error('[INTERNAL_ERROR] Could not find hash for game ', gameId);
            return callback('NO_GAME_HASH');
        }

        var hash = results.rows[0].hash;
        var gameCrash = lib.crashPointFromHash(hash);
        assert(lib.isInt(gameCrash));

        query('INSERT INTO games(id, game_crash) VALUES($1, $2)',
            [gameId, gameCrash], function(err) {
                if (err) return callback(err);

                return callback(null, { crashPoint: gameCrash, hash: hash } );
            });
    });
};

exports.getBankroll = function(callback) {
    query('SELECT (' +
            '(SELECT COALESCE(SUM(amount),0) FROM fundings) - ' +
            '(SELECT COALESCE(SUM(balance_satoshis), 0) FROM users)) AS profit ',
        function(err, results) {
            if (err) return callback(err);

            assert(results.rows.length === 1);

            var profit = results.rows[0].profit - 100e8;
            assert(typeof profit === 'number');

            var min = 1e8;

            callback(null, Math.max(min, profit));
        }
    );

};

exports.getGameHistory = function(callback) {

    query('SELECT games.id game_id, games.game_crash, games.created, (array_agg(game_hashes.hash))[1] AS hash, ' +
    ' json_agg(pv) plays ' +
        'FROM games ' +
        'LEFT JOIN (SELECT users.username, plays.bet, plays.cash_out, plays.bonus, plays.game_id ' +
        '  FROM plays, users ' +
        '  WHERE plays.user_id = users.id) pv ON pv.game_id = games.id ' +
        'LEFT JOIN game_hashes ON games.id = game_hashes.game_id ' +
        'WHERE games.ended = true ' +
        'GROUP BY 1 ' +
        'ORDER BY games.id DESC LIMIT 10;', function(err, data) {
            if (err) throw err;

        data.rows.forEach(function(row) {
                row.player_info = {};

                row.plays.forEach(function(play) {
                    if (!play) return;

                    // The database does not store the stopped_at value,
                    // so we recalculate it.
                    var stopped_at = Math.round(100 * play.cash_out / play.bet);
                    row.player_info[play.username] =
                        { bet: play.bet,
                          stopped_at: stopped_at,
                          bonus: play.bonus
                        };
                });

                delete row.plays;
            });
            callback(null, data.rows);
        });
};

