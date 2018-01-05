
const express = require("express");
const async = require('async');
const uuid = require ('uuid');
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
const docClient = new AWS.DynamoDB.DocumentClient();
let app = express();
let jobStack = [];  // a stack of request parameters for batchWrites
let itemCount = 0;  // total number of items processed
const propertyTable = 'rv_property';
const max_reqs = 25;
var params = {
    TableName: "open_data",
};
// parameter template for batchWrite()

let chunkNo = 0;  // number of each batchWrite operation
app.get('/', function (err, res) {
    docClient.scan(params, onScan);
    function onScan(err, data) {
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
          
          // construct job stack
    while (data.Items.lenght > 0) {
        console.log(data.Count);
        // parameter template for batchWrite()
        const propertyparams = {
            RequestItems: {
                [propertyTable]: []
            }
        }
        // extract 25 items from start of data, then create a new job with 25 put requests
        data.Items.splice(0, max_reqs).forEach((itemdata) => {

           var obj = {
                'P_ID':uuid.v1(),
                'propertyId'  :parseInt(itemdata.ain,10),
                'P_Ownership_Period' :itemdata.roll_landbaseyear,
                'P_Lat' : itemdata.location_1.latitude,
                'P_Long' : itemdata.location_1.longitude,
                'P_Address_Line1' : itemdata.propertylocation,
                'P_Address_Line2' : itemdata.location_1.human_address,
                'P_Zipcode' : itemdata.situszip5,
                'P_City' : itemdata.location_1.human_address,
                'P_County' : 'Los Angeles',
                'P_State' : 'test',
                'P_Country' : 'USA',
                'P_Tax_Amount' : itemdata.nettaxablevalue,
                'P_Mortgage' : itemdata.nettaxablevalue / itemdata.roll_landbaseyear,
                'P_Year_Bulit' : itemdata.effectiveyearbuilt,
                'P_Sqft' : itemdata.sqftmain,
                'P_Bedrooms' : itemdata.bedrooms,
                'P_Bathrooms' : itemdata.bathrooms,
                'P_Insurance_Cost':null,
                'P_ Availability': null ,
                'P_Complaints':[] ,
                'P_Rentals': [],
                'P_Landlords' : [],
                'P_Reviews':[] ,
                'P_Photos': null ,
                'P_Avg_Rating':null ,
                'P_Approval_Rate' :null , 
                'P_Created_By':'Data Import',
                'P_Updated_By': 'Data Import',
                'P_Created_On': '1-1-2018',
                'P_Updated_On':'1-1-2018',
                'P_Zoning': null
            }
         

            propertyparams.RequestItems[propertyTable].push({
                    PutRequest: {
                        Item: obj
                    }
                });
                itemCount++;
   
        });
        jobStack.push(propertyparams);  // push this job to the stack
    }    
                  
            async.each(jobStack, (params, callback) => {
                docClient.batchWrite(params, callback);
            }, (err) => {
                if (err) {
                    console.log(`Chunk #${chunkNo} write unsuccessful: ${err.message}`);
                } else {
                    console.log('\nImport operation completed! Do double check on DynamoDB for actual number of items stored.');
                    console.log(`Total batchWrite requests issued: ${chunkNo}`);
                    console.log(`Total valid items processed: ${itemCount}`);
                }
            });

        }
    }

})
app.listen(3000, function () {
    console.log("server listeing on port 3000")
});






