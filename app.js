var Twitter = require('twitter');
var config = require('config-heroku');
var CartoDB = require('cartodb');
var fs = require('fs');
var util = require('util');
var http = require('http');
var seqqueue = require('seq-queue');
var request = require('request');
var express = require('express')
var app = express();
var CronJob = require('cron').CronJob;
var WORKERS = process.env.WEB_CONCURRENCY || 1;
var throng = require('throng');


function start() {
  console.log("test");
  var twitterClient = new Twitter({
    consumer_key: config['Twitter'].api_key,
    consumer_secret: config['Twitter'].api_secret,
    access_token_key: config['Twitter'].access_token,
    access_token_secret: config['Twitter'].access_token_secret
  });

  var cartodbWriteClient = new CartoDB({
    user: config['CartoDB'].write_username,
    api_key: config['CartoDB'].write_api_key
  });

  var cartodbReadClient = new CartoDB({
    user: config['CartoDB'].read_username
  });

  function getVizJson(callback) {
    cartodbReadClient.query("SELECT url FROM {table} ORDER BY count DESC LIMIT 1", { table: 'top_viz' }, function(err, data){
      if (data.rows && data.rows.length === 1) {
        var url = data.rows[0].url;
        var data = /https:\/\/(.+)\.cartodb.com\/api\/v2\/viz\/(.+)\/viz.json/.exec(url);
        callback && callback({ username: data[1], id: data[2] });
      }
    });
  }

  function mapReported(data, callback) {
    cartodbWriteClient.query("SELECT CASE WHEN exists (SELECT true FROM {table} WHERE map_id='{id}') THEN 'true' ELSE 'false' END", { table: 'trendy_maps_list', id: data.id }, function(err, data){
      var reported = data && data.rows && data.rows[0].case === 'true';
      callback && callback(reported);
    });
  }

  function insertMapId(data, callback) {
    cartodbWriteClient.query("INSERT INTO {table} (map_id) VALUES ('{id}');", { table: 'trendy_maps_list', id: data.id }, function(err, data){
      callback && callback();
    });
  }

  function postTweet(mapData, callback) {
    var msg = 'Currently CartoDB top map: http://' + mapData.username + '.cartodb.com/viz/' + mapData.id + '/public_map';
    
    twitterClient.post('statuses/update', { status: msg },  function(error, tweet, response){
      callback && callback();
    });
  }

  function changeBackground(mapData, callback) {

    var download = function(uri, filename, callback){
      request.head(uri, function(err, res, body){
        var r = request(uri).pipe(fs.createWriteStream(filename));
        r.on('close', callback);
      });
    };

    var img = 'http://' + mapData.username + '.cartodb.com/api/v2/viz/' + mapData.id + '/static/1400/300.png';

    download(img, 'bkg.png', function(){
      var data = require('fs').readFileSync('bkg.png');
      twitterClient.post('account/update_profile_banner', { banner: data, media: data },  function(error, tweet, response){
        callback && callback();
      });
    });
  }


  var job = new CronJob(config['Cron'], function() {
    console.log("Checking new map...");

    var queue = seqqueue.createQueue(10000);
    var mapData = {};

    // Get trend map!
    queue.push(
      function(task) {
        getVizJson(function(d) {
          console.log("Map data: " + mapData);
          mapData = d;
          task.done();
        });
      }, 
      function() {}, 
      1000
    );

    // Already reported?
    queue.push(
      function(task) {
        mapReported(mapData, function(reported) {
          mapData.reported = reported;
          task.done();
        });
      },
      function() {},
      1000
    );

    // Store it if it was not reported
    queue.push(
      function(task) {
        if (mapData.reported) {
          task.done();
        } else {
          insertMapId(mapData, function() {
            console.log("New map stored!: " + mapData);
            task.done();
          })
        }
      },
      function() {},
      1000
    );

    // Send tweet if it was not reported
    queue.push(
      function(task) {
        if (mapData.reported) {
          task.done();
        } else {
          postTweet(mapData, function() {
            console.log("Tweet posted!: " + mapData);
            task.done();
          });
        }
      },
      function() {},
      1000
    );

    // Change background
    queue.push(
      function(task) {
        if (mapData.reported) {
          task.done();
        } else {
          changeBackground(mapData, function() {
            console.log("Banner changed!: " + mapData);
            task.done();
          });
        }
      },
      function() {},
      6000
    );

  }, null, true, 'America/Los_Angeles');

  app.set('port', (process.env.PORT || 5000))
  app.use(express.static(__dirname + '/public'))

  app.get('/', function(request, response) {
    response.send('Working!')
  });

  app.listen(app.get('port'), function() {
    console.log("Node app is running at localhost:" + app.get('port'))
  });
}

throng(start, {
  workers: WORKERS,
  lifetime: Infinity
});