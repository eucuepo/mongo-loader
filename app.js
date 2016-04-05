var mongoClient = require('mongodb').MongoClient;
var async = require('async');
var url = 'mongodb://localhost:27017';

// config
var config = {
	DB_URL: 'mongodb://localhost:27017', // DB base URL
	DATABASE_NUMBER:100, // db number
	COLLECTIONS_NUMBER:100, // collections on each db
	OBJECTS_NUMBER:10, // objects on each collection
	FIELDS_NUMBER:5, // fields on each object
	ALLOW_SUBDOCS:false, // allow subdocuments on objects
	MAX_CONCURRENCY:10 // max queue size
}

var q = async.queue(function (task, taskDoneCallback) {
    populateDB(taskDoneCallback);
}, config.MAX_CONCURRENCY);

for(var i=0; i<config.DATABASE_NUMBER; i++){
	q.push({});
}

// helper functions
var insertDocument = function(db, collectionName,fields,allowSubdocs, callback) {
   db.collection(collectionName).insertOne(createRandomObj(fields,allowSubdocs), function(err, result) {
    callback();
  });
};

var populateDB = function(taskDoneCallback) {
	// random db
	var dbName = config.DB_URL + '/' + randomString(10);
	console.log('Populating ' + dbName +' database');
	
	mongoClient.connect(dbName, function(err, db) {
		async.times(config.COLLECTIONS_NUMBER, function(j, nextCollection){
			var collectionName = randomString(10);
			async.times(config.OBJECTS_NUMBER, function(k, nextObject){
			  insertDocument(db, collectionName, config.FIELDS_NUMBER,config.ALLOW_SUBDOCS,
			   function() {
			   	  console.log('document inserted');
			      nextObject();
			  });
			},function(){
				console.log('Collection created');
				// objects completed
				nextCollection();
			});
		}, function() {
		  // collections done, close DB
		  console.log('Closing db for ' + db.databaseName);
		  db.close();
		  taskDoneCallback();
		});
	});
}

var createRandomObj = function(fieldCount, allowNested) {
    var generatedObj = {};

    for(var i = 0; i < fieldCount; i++) {
        var generatedObjField;

        switch(randomInt(allowNested ? 6 : 5)) {

            case 0:
            generatedObjField = randomInt(1000);
            break;

            case 1:
            generatedObjField = Math.random();
            break;

            case 2:
            generatedObjField = Math.random() < 0.5 ? true : false;
            break;

            case 3:
            generatedObjField = randomString(randomInt(4) + 4);
            break;

            case 4:
            generatedObjField = null;
            break;

            case 5:
            generatedObjField = createRandomObj(fieldCount, allowNested);
            break;
        }
        generatedObj[randomString(8)] = generatedObjField;
    }
    return generatedObj;
}

function randomInt(rightBound)
{
    return Math.floor(Math.random() * rightBound);
}

function randomString(size)
{
    var alphaChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    var generatedString = '';
    for(var i = 0; i < size; i++) {
        generatedString += alphaChars[randomInt(alphaChars.length)];
    }

    return generatedString;
}