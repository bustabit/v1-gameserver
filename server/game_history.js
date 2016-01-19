var CBuffer = require('CBuffer');
var config = require('./config');
var database = require('./database');
var _ = require('lodash');

function GameHistory (gameTable) {
    var self = this;
    self.gameTable = new CBuffer(config.GAME_HISTORY_LENGTH);
    gameTable.forEach(function(game) {
        self.gameTable.push(game);
    });
}

GameHistory.prototype.addCompletedGame = function (game) {
    this.gameTable.unshift(game);
};

GameHistory.prototype.getHistory = function () {
    return this.gameTable.toArray();
};

module.exports = GameHistory;
