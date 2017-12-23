const express = require("express");
const AWS = require("aws-sdk");
const uuid = require("uuid");
const csv_filename = "./JIRA.csv";
let app = express();
const fs = require('fs');
const parse = require('csv-parse');

AWS.config.loadFromPath('config.json');


const DynamoDB = new AWS.DynamoDB.DocumentClient();


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
        rs = fs.createReadStream(csv_filename);
        parser = parse({
            columns : true,
            delimiter : ','
        }, function(err, data) {
            
            console.log(data);
        
           for (var i = data.length - 1; i >= 0; i--) {
                
                   var obj = {};
                    //Add the Unique ID which is the PK in Dynamo DB and we dont recive any unique data from SODA 
                   
              obj = data[i];
              obj.ID = uuid.v1();
                    var params = {
                        TableName: "test",
                        Item: obj
                    };
                    //Pull data In Daynamo 
                   PullData(params);
            };
            
            });
            rs.pipe(parser);
            res.send("Success");
       
    } catch (err) {
        res.send(err);
    }
});



app.listen(5000, function () {
    console.log('Example app listening on port 5000!');
});





