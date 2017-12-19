const express = require("express");
const request = require("request-promise");
const AWS = require("aws-sdk");
const uuid = require("uuid");

let app = express();

//setup  region 
AWS.config.update({
    region: "us-east-1"
});

const DynamoDB = new AWS.DynamoDB.DocumentClient();

let GetSodaData = function () {
    return request({
        url: 'https://data.cityofgainesville.org/resource/s829-win5.json',
        headers: {
            'X-App-Token': 'hcNtgiqH88arSbAbOYTMoU5Nv',
            'Content-type': 'application/json'
        }
    }).catch(function (err) {
        console.log(err);
    });
}

let PullData = function (data) {
    DynamoDB.put(data, function (err, data) {
        if (err) {
            console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Added item:", JSON.stringify(data, null, 2));
        }
    });

}

app.get('/', function (req, res) {

    try {
        GetSodaData().then(function (response) {
            //const response = await GetSodaData();
            let json = JSON.parse(response);

            Object.keys(json).forEach(function (i, callback) {

                var data = json[i];
                //object will be coulm name and column value as it is a dynamic returned data
                var obj = {};
                for (i in data) {
                    obj[i] = data[i];
                }

                //Add the Unique ID which is the PK in Dynamo DB and we dont recive any unique data from SODA 
                obj.ID = uuid.v1();

                var params = {
                    TableName: "Soda_Data",
                    Item: obj
                };
                //Pull data In Daynamo 
                PullData(params);
            });
            res.send("Success");
        });
    } catch (err) {
        res.send(err);
    }
});



app.listen(3000, function () {
    console.log('Example app listening on port 3000!');
});





