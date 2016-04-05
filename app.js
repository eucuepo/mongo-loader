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
  MAX_DATABASE_CONCURRENCY:10, // max database creator queue size
  MAX_COLLECTION_CONCURRENCY:10 // max collection creator queue size
}

var databaseQueue = async.queue(function (task, taskDoneCallback) {
  populateDB(task.dbIndex,taskDoneCallback);
}, config.MAX_DATABASE_CONCURRENCY);

for(var i=0; i<config.DATABASE_NUMBER; i++){
  databaseQueue.push({dbIndex:i});
}

// helper functions
var insertDocument = function(db, collectionName,fields,allowSubdocs, callback) {
  db.collection(collectionName).insertOne(createRandomObj(fields,allowSubdocs), function(err, result) {
    callback();
  });
};

var populateDB = function(dbIndex,taskDoneCallback) {
  // random db
  var dbName = config.DB_URL + '/' + randomString(10);
  console.log('Populating ' + dbName +' database');
  mongoClient.connect(dbName, function(err, db) {
    var collectionQueue = async.queue(function (task, collectionDoneCallback) {
	  populateCollection(collectionDoneCallback, task.db,task.colIndex,task.dbIndex);	
	}, config.MAX_COLLECTION_CONCURRENCY);


	collectionQueue.drain = function() {
	  // once queue completed, close db connection
	  console.log('Closing db connection for ' + db.databaseName);
	  db.close();
	  taskDoneCallback();
	}

	for(var j=0;j<config.COLLECTIONS_NUMBER;j++){
	  collectionQueue.push({db:db,colIndex:j,dbIndex:dbIndex});
	}

  });
}

var populateCollection = function(collectionDoneCallback,db,colIndex,dbIndex){
	var collectionName = randomString(10);
	async.times(config.OBJECTS_NUMBER, function(k, nextObject){
	  insertDocument(db, collectionName, config.FIELDS_NUMBER,config.ALLOW_SUBDOCS,
	   function() {
	      nextObject();
	  });
	},function(){
		console.log('Collection '+ colIndex +' created for database ' + dbIndex + ' named '+collectionName);
		// collection done
		collectionDoneCallback();
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