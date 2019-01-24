var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var mysql = require("mysql");
var http = require("http");

var app = express();

// kafka
var kafka = require('kafka-node')
var client = new kafka.KafkaClient("localhost:2181")
var Producer = kafka.Producer
var producer = new Producer(client)

producer.on("ready", function(){
   console.log("ready for send msg to kafka")
})

producer.on("error", function(err) {
    console.log("not connected to kafka")
    console.log(err)
});


// mysql
var db = mysql.createConnection({
  user: "root",
  password: "khw1085741",
  database: "edu_cam"
})


// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));


// main - login
app.post('/auth', function(req, res, next){
  var params = [req.body.email_id, req.body.password]
  var result = db.query("SELECT token FROM USER_TOKEN WHERE ID=? AND PW=?", params, function(err, result, fields){
        if(err){
                console.log(err)
                res.status(500).send("DB Error")
        }else{
                res.json({success: true, token : result[0].token})
        }

  })
});


// main - stream
app.post('/stream', function(req, res, necxt){
  console.log("msg is arrived from app")
  var msg = req.body.msg
  var token = req.body.token
  var payloads = [{topic : "stream1",
                  messages : [msg],
  		  key : token}]

  producer.send(payloads, function(err, data){
    if(err){	    
      console.log("fail to send msg to kafka")
      console.log(err)
    }else{
      console.log(token)
      console.log("msg is sent to kafka")
      res.json({success : true})
    }
  })
});


module.exports = app;
