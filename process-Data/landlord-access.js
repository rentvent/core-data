const async = require('async');
const Promise = require('bluebird');
const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
const db = new AWS.DynamoDB({});
const uuid = require('uuid');

const params = {
    TableName: "Accesor_Staging"
};

let jobStack = [];  // a stack of request parameters for batchWrites
const propertyTable = 'rv_landlord';
var prop = [];
var obj = {};
var lanList = [];
var size = 25;

const propertyparams = {
    RequestItems: {
        [propertyTable]: []
    }
};

let saveData = function () {

   var  index = 0;
    while (lanList.length > 25) {
        var arrayOf25 = lanList.splice(index, 25);
        var itemsArray = [];
        for (i = 0; i < arrayOf25.length; i++) {
            var item = {
                PutRequest: {
                    Item: arrayOf25[i]
                }
            };
            if (item)
                itemsArray.push(item);
        }

        var params = {
            RequestItems: {
                'rv_landlord': itemsArray
            }
        };
        db.batchWriteItem(params, function (err, data) {
            if (err) console.log(err); // an error occurred
            else console.log(data); // successful response
        });
    }
    if(lanList.length >0){
        console.log(lanList.length);
    }

};

let GetAccessData = function () {
    return db.scan(params).promise().then(function (data, err) {
        if (err != null)
            return;
        else {

            console.log("Element : " + data.Items.length);
            data.Items.forEach(function (element) {
                prop = [];
                prop.push({ "M": { 'p_id': element.IdentificationNumber } });

                //search for one landlord that has more than one property
                data.Items.forEach(function (temp, index) {

                    if (element["First Owner Assessee Name"].S == temp["First Owner Assessee Name"].S
                        && temp.IdentificationNumber.N != element.IdentificationNumber.N) {

                        prop.push({ "M": { 'p_id': temp.IdentificationNumber } });
                        data.Items.splice(index, 1);
                    }

                });

                var landlorld = {
                    'landlord_id': { "S": uuid.v1() },
                    'L_Full_Name': element["First Owner Assessee Name"],
                    'L_Address_Line1': element["Mail Address House Number"],
                    'L_Address_Line2': element["Mail Address Street Name"],
                    'L_City': element["Mail Address City and State"],
                    'L_Zipcode': element["Mail Address Zip Code"],
                    'L_County': { "S": "Los Angeles" },
                    'L_State': element["Mail Address City and State"],
                    'L_Country': { "S": "USA" },
                    'L_Properties': { "L": prop },
                    'L_Created_By': { "S": "Data_Import" },
                    'L_Updated_By': { "S": "Data_Import" },
                    'L_Created_On': { "S": "22-01-2018" },
                    'L_Updated_On': { "S": "22-01-2018" }
                };
                lanList.push(landlorld);
            });
        }
        saveData();
    })
};

GetAccessData();






