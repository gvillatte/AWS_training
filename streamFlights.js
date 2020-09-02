
const https = require('https');
// Load the SDK and UUID
var AWS = require('aws-sdk');
var uuid = require('node-uuid');

AWS.config.region = 'eu-west-3';
AWS.config.credentials.get(function(err) {
    // attach event listener
    if (err) {
        alert('Error retrieving credentials.');
        console.error(err);
        return;
    }
});

 // create Amazon Kinesis service object
 var kinesis = new AWS.Kinesis({
        apiVersion: '2013-12-02'
    });
	

function getFlights()
{
	//get flights data over the bigger Paris area
https.get('https://opensky-network.org/api/states/all?lamin=48.27&lomin=1.47&lamax=49.2&lomax=3.5', (resp) => {
  let data = '';

  // A chunk of data has been recieved.
  resp.on('data', (chunk) => {
    data += chunk;
  });

  // The whole response has been received. Print out the result.
  resp.on('end', () => {

//parse the JSON object	  
var parsed = JSON.parse(data);
var states = parsed.states;

//Loop on all the states objects to get the info we need 
for (let key in states) {
	var flight = states[key];
	var  idflight = flight[0];
	var  countryflight = flight[2];
	var dateflight = flight[3];
	var longitude = flight[5];
	var latitude = flight[6];
	var geo_point=''+latitude+','+longitude;

	var aircrafts = {
		date: dateflight,
		location: geo_point,
		id:idflight,
		country:countryflight
	};
//Create the output JSON object	
	var record = {
				Data: JSON.stringify({aircrafts
                }),
                PartitionKey: 'partition-' + 'key1'
            };
	var recordData = [];
	recordData.push(record);
  }
console.log('Putting ' + recordData.lenght + ' records to kinesis');
//send the data to Kinesis		
 kinesis.putRecords({
				Records: recordData,
				StreamName: 'flight_stream'
        }, function(err, data) {
            if (err) {
                console.error(err);
            }
        });
  
 });
console.log('Finished putting record');

}).on("error", (err) => {
  console.log("Error: " + err.message);
});
}

setInterval(getFlights, 5000);