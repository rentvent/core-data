
const request = require("request-promise");
const AWS = require("aws-sdk");
const async = require('async');
let itemCount = 0;
let chunkNo = 0;

//setup  region
AWS.config.update({
    region: "us-east-1"
});
let jobStack = [];  // a stack of request parameters for batchWrites
const DynamoDB = new AWS.DynamoDB.DocumentClient();

let GetSodaData = function () {
    return request({
        url: 'https://data.lacounty.gov/resource/7rjj-f2pv.json?$limit=50000',
        headers: {
            'X-App-Token': 'hcNtgiqH88arSbAbOYTMoU5Nv',
            'Content-type': 'application/json'
        }
    }).catch(function (err) {
        console.log(err);
    });
}

let ProcessData = function () {


    GetSodaData().then(function (response) {
        //const response = await GetSodaData();
        let json = JSON.parse(response);


        while (json.length > 0) {

            const params = {
                RequestItems: {
                    ['Open_Data']: []
                }
            }


            json.splice(0, 25).forEach(function (data) {

                data.ain = parseInt(data.ain);
                params.RequestItems['Open_Data'].push({
                    PutRequest: {
                        Item: data
                    }
                });

                itemCount++;
            });

            jobStack.push(params);  // push this job to the stack
            chunkNo++;

        }
    var  issuedPram ;
        async.each(jobStack, (params, callback) => {
            DynamoDB.batchWrite(params, callback);
            issuedPram =params ;
        }, (err) => {
            if (err) {

                console.log(`Chunk #${chunkNo} write unsuccessful: ${err.message}`);
                console.log(issuedPram);
            } else {
                console.log('\nImport operation completed! Do double check on DynamoDB for actual number of items stored.');
                console.log(`Total batchWrite requests issued: ${chunkNo}`);
                console.log(`Total valid items processed: ${itemCount}`);
            }
        });
    });
}

ProcessData();