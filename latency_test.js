var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
const percentile = require("percentile");

// Configuration
var connectionString = "mongodb+srv://********:***********@searchlatencytest.5tka5.mongodb.net/myFirstDatabase"
var dbName = "sample_mflix"
var collectionName = "movies"
var numberOfUpdatesPerIteration = 10
var maximumNumberOfIteration = 10
var searchIndexName = 'default'
var fieldToBeUpdated = 'plot' // this is also field to be searched

function randomString(len, an) {
    an = an && an.toLowerCase();
    var str = "",
      i = 0,
      min = an == "a" ? 10 : 0,
      max = an == "n" ? 10 : 62;
    for (; i++ < len;) {
      var r = Math.random() * (max - min) + min << 0;
      str += String.fromCharCode(r += r > 9 ? r < 36 ? 55 : 61 : 48);
    }
    return str;
}

MongoClient.connect(connectionString, async function(err, dbconn) {
    if (err) throw err;

    var db = dbconn.db(dbName);

    var iterationNumber = 0; // current iteration number

    var durations = []

    var stats = await db.collection(collectionName).stats()
    console.log("Collection stats: ")
    console.log(`# of docs: ${stats.count}`)
    console.log(`avg obj size: ${stats.avgObjSize}`)
    console.log(`# of WT indexes: ${stats.nindexes}`)
    while (true) {
        console.log("======================================================================")
        iterationNumber++
        console.log("Iteration number:" + iterationNumber)

        var randomData = randomString(7, 'a') + " " + randomString(10, 'a') + " " + randomString(5, 'a')
        var updateObject = {}
        updateObject[fieldToBeUpdated] = randomData
        var searchAggregationStage = {
            '$search' : {
                'index' : searchIndexName,
                'phrase': {
                    'query': randomData, 
                    'path': fieldToBeUpdated
                }
            }
        }

        //var _ids = await db.collection(collectionName).find({'_id': {$gt: startId} },{'_id':1}).sort({'_id':1}).limit(numberOfUpdatesPerIteration).toArray()

        var _ids = await db.collection(collectionName).aggregate(
            [
                {'$sample': { 'size' : numberOfUpdatesPerIteration}},
                {'$project' : {'_id': 1}},
                {'$sort' : {'_id': 1}}
            ]).toArray()

        if (iterationNumber >= maximumNumberOfIteration ) {
            console.log("Maximum iteration limit is reached, it's been completed.")
            break;
        }

        // convert from array of objects
        var _idsConverted = _ids.map(x => x._id) 
        //console.log("First _id in this iteration:" + _numericIds[0] + ";Last _id in this iteration:" + _numericIds[_numericIds.length-1])

        var result = await db.collection(collectionName).updateMany(
            {"_id": {$in: _idsConverted}}, 
            [{"$set": updateObject}
        ])
        console.log(`Update finished >>> Matched Count: ${result.matchedCount}; Modified Count ${result.modifiedCount}`)
        
        // update completed now we will start timer and 
        // execute $search queries until the returned _ids from $search match with the updated documents' _ids
        var starttime= moment()

        // execute $search until found all the updated ids data 
        var indexSearchIteration = 0
        while (true) {
            indexSearchIteration++
            var searchIndexUpdatedArray = await db.collection(collectionName).aggregate([
                searchAggregationStage,
                {
                    '$project' : {
                        '_id' : 1
                    }
                },
                {
                    '$sort' : {
                        '_id' : 1
                    }
                }
            ]).toArray()

            var _idsConvertedAfterSearch = searchIndexUpdatedArray.map(x => x._id) 

            // verify that returned _ids from $search are the same that we updated 
            // below checks the array elements are same
            if (JSON.stringify(_idsConvertedAfterSearch) === JSON.stringify(_idsConverted)) {
                // index was updated properly
                console.log(`At the iteration ${indexSearchIteration}, search index was found updated.`)
                break;
            } else {
                // console.log("Search query executed but data was not found -- index is still being updated.")
            }
        }

        // measuring time
        var endtime= moment();
        var diff = moment.duration(endtime.diff(starttime));
        console.log("This iteration has been completed in:" + (diff) +" milliseconds.")
        durations.push(diff)
    }
    console.log("Stats:")
    console.log(`Max execution time: ${Math.max(...durations)} ms`)
    console.log(`Max execution time: ${Math.min(...durations)} ms`)
    
    const percentileResult = percentile(
        [50, 95, 99], // calculates 50p, 95p and 99p in one pass
        durations
      );
    
    console.log("Mean execution time:" + percentileResult[0] + " ms")
    console.log("%95 execution time:" + percentileResult[1] + " ms")
    console.log("%99 execution time:" + percentileResult[2] + " ms")

    process.exit(0)


});