
var AWS = require('aws-sdk');
AWS.config.region = "us-west-2";
AWS.config.loadFromPath('config.json');
var dbClient = new AWS.DynamoDB();


var params = {
    TableName : "test",
    KeySchema: [       
          { AttributeName: "ID", KeyType: "HASH"}, 
    ],
    AttributeDefinitions: [       
        { AttributeName: "ID", AttributeType: "S" }
    ],
    ProvisionedThroughput: {       
        ReadCapacityUnits: 10, 
        WriteCapacityUnits: 10
    }
};

dbClient.createTable(params, function(err, data) {
    if (err) {
        console.error("Unable to create table. Error JSON:", JSON.stringify(err));
    } else {
        console.log("Created table");
    }
});