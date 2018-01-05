const fs = require('fs');
const csvParser = require('csv-parse');
const crypto = require('crypto');
const async = require('async');

const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
const dynamo = new AWS.DynamoDB.DocumentClient({ convertEmptyValues: true });

// ** Set up variables here ********************************
const csv_filename = '/Users/ad/Workspace/core-data/Assessor_Data/chunk-2500-v2.csv';
const table_name = 'Accesor_Staging';

// additional properties to be included
const is_dynamic = 'true';
// *********************************************************

const rs = fs.createReadStream(csv_filename);
const parser = csvParser({
    auto_parse: true,
    columns : true,
    delimiter : ','
}, function(err, data) {
    const max_reqs = 25;  // maximum put requests for one batchWrite() allowed by AWS
    let itemSet = {};  // the raw csv dataset contains duplicates; used this as a hash set to detect duplicates
    let jobStack = [];  // a stack of request parameters for batchWrites
    let itemCount = 0;  // total number of items processed
    let duplicateCount = 0;  // total number of duplicate items

    // construct job stack
    while (data.length > 0) {
        // parameter template for batchWrite()
        const params = {
            RequestItems: {
                [table_name]: []
            }
        }
        // extract 25 items from start of data, then create a new job with 25 put requests
        data.splice(0, max_reqs).forEach((item) => {
            //let id = item.AssessorIdentificationNumber;

                params.RequestItems[table_name].push({
                    PutRequest: {
                        Item: item
                    }
                });
                itemCount++;
   
        });
        jobStack.push(params);  // push this job to the stack
    }

    
    let chunkNo = 0;  // number of each batchWrite operation

    // issue batchWrite() for each job in the stack; use async.js to better handle the asynchronous processing
    async.each(jobStack, (params, callback) => {
        //console.log(JSON.stringify(params), "\n");
        chunkNo++;
        dynamo.batchWrite(params, callback);
        //callback();
    }, (err) => {
        if (err) {
            console.log(`Chunk #${chunkNo} write unsuccessful: ${err.message}`);
        } else {
            console.log('\nImport operation completed! Do double check on DynamoDB for actual number of items stored.');
            console.log(`Total batchWrite requests issued: ${chunkNo}`);
            console.log(`Total valid items processed: ${itemCount}`);
            console.log(`Total number of duplicates in the raw data: ${duplicateCount}`);
        }
    });

});
rs.pipe(parser);  // pipe the file readable stream to configured csv parser
