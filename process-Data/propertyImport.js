const async = require('async');
const date = require('date-and-time');

const AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
const docClient = new AWS.DynamoDB.DocumentClient();


let jobStack = [];  // a stack of request parameters for batchWrites
let itemCount = 0;  // total number of items processed
const max_reqs = 25;

const params = {
    TableName: 'Open_Data'
};

var item = [] ;

let chunkNo = 0;  // number of each batchWrite operation

let ProcessData = function () {

    docClient.scan(params, onScan);

    function onScan(err, data) {
        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        }
        else {

            item = item.concat(data.Items);


            if (typeof data.LastEvaluatedKey != "undefined") {
                console.log("Scanning for more...");
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                ProcessData();
            }

            else {

                while (item.length > 0) {


                    const propertyparams = {
                        RequestItems: {
                            ['rv_property']: []
                        }
                    }
                    item.splice(0, max_reqs).forEach((itemdata) => {
                        var propertyObj = {
                            'P_ID': itemdata.ain,
                            'P_Ownership_Period': itemdata.roll_landbaseyear,
                            'P_Lat': itemdata.location_1.latitude,
                            'P_Long': itemdata.location_1.longitude,
                            'P_Address_Line1': itemdata.propertylocation,
                            'P_Address_Line2': itemdata.location_1.human_address,
                            'P_Zipcode': itemdata.situszip5,
                            'P_City': itemdata.location_1.human_address,
                            'P_County': 'Los Angeles',
                            'P_State': itemdata.location_1.human_address,
                            'P_Country': 'USA',
                            'P_Tax_Amount': itemdata.nettaxablevalue,
                            'P_Mortgage': (isNaN(Math.round(itemdata.nettaxablevalue / itemdata.roll_landbaseyear))) ? 0  : Math.round(itemdata.nettaxablevalue / itemdata.roll_landbaseyear) ,
                            'P_Year_Bulit': itemdata.effectiveyearbuilt,
                            'P_Sqft': itemdata.sqftmain,
                            'P_Bedrooms': itemdata.bedrooms,
                            'P_Bathrooms': itemdata.bathrooms,
                            'P_Insurance_Cost': null,
                            'P_ Availability': null,
                            'P_Complaints': [],
                            'P_Rentals': [],
                            'P_Landlords': [],
                            'P_Reviews': [],
                            'P_Photos': null,
                            'P_Avg_Rating': null,
                            'P_Approval_Rate': null,
                            'P_Created_By': 'Data Import',
                            'P_Updated_By': 'Data Import',
                            'P_Created_On': date.format(new Date(), 'DD-MM-YYYY'),
                            'P_Updated_On': date.format(new Date(), 'DD-MM-YYYY'),
                            'P_Zoning': null
                        }

                        propertyparams.RequestItems['rv_property'].push({
                            PutRequest: {
                                Item: propertyObj
                            }
                        });
                        itemCount++;
                    });
                    chunkNo++;
                    jobStack.push(propertyparams);  // push this job to the stack


                }
                async.each(jobStack, (propertyparams, callback) => {
                docClient.batchWrite(propertyparams, callback);

                }, (err) => {

                    if (err) {

                        console.log(`Chunk #${chunkNo} write unsuccessful: ${ err.stack}`);

                    } else {
                        console.log('\nImport operation completed! Do double check on DynamoDB for actual number of items stored.');
                        console.log(`Total batchWrite requests issued: ${chunkNo}`);
                        console.log(`Total valid items processed: ${itemCount}`);
                    }
                });
            }

        }

    }

}

ProcessData();


