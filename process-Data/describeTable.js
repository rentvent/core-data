const AWS = require("aws-sdk");
//setup  region
AWS.config.update({
    region: "us-east-1"
});
const  dynamodb = new AWS.DynamoDB();



var params = {
    TableName: "rv_landlord"
};
dynamodb.describeTable(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else console.log(data);
});

