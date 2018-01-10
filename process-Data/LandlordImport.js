const async = require('async');
const date = require('date-and-time');
const uuid = require('uuid');
const AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
const docClient = new AWS.DynamoDB.DocumentClient();


let jobStack = [];  // a stack of request parameters for batchWrites
let itemCount = 0;  // total number of items processed
const max_reqs = 25;

const params = {
    TableName: 'Accesor_Merge_Data'
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

                while (data.Items.length > 0) {


                    const Landlordparams = {
                        RequestItems: {
                            ['rv_landlord']: []
                        }
                    };

                    data.Items.splice(0, max_reqs).forEach((element) => {


                        var str =element.Properties;
                        var newstr = str.replace(new RegExp( 'p_id', 'g'), '"p_id"');
                        var jsonObj = JSON.parse(newstr);


                        var LandlordObj = {
                            'L_ID': uuid.v1(),
                            'L_Full_Name': element["First Owner Assessee Name"],
                            'L_Address_Line1': element["Mail Address House Number"],
                            'L_Address_Line2': element["Mail Address Street Name"],
                            'L_City': element["Mail Address City and State"],
                            'L_Zipcode': element["Mail Address Zip Code"],
                            'L_County': "Los Angeles",
                            'L_State': element["Mail Address City and State"],
                            'L_Country': "USA",
                            'L_Properties': jsonObj ,
                            'L_Created_By': "Data_Import",
                            'L_Updated_By': "Data_Import",
                            'L_Created_On':  date.format(new Date(), 'DD-MM-YYYY') ,
                            'L_Updated_On':  date.format(new Date(), 'DD-MM-YYYY')
                        }

                        Landlordparams.RequestItems['rv_landlord'].push({
                            PutRequest: {
                                Item: LandlordObj
                            }
                        });
                        itemCount++;
                    });
                    chunkNo++;
                    jobStack.push(Landlordparams);  // push this job to the stack


                }
                async.each(jobStack, (Landlordparams, callback) => {
                    docClient.batchWrite(Landlordparams, callback);

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


        if (typeof data.LastEvaluatedKey != "undefined") {
            console.log("Scanning for more...");
            params.ExclusiveStartKey = data.LastEvaluatedKey;
            ProcessData();
        }


    }

}

ProcessData();


