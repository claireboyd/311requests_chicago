'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
const readline = require("readline");
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		// if length > 5, then this is a binary number and should be converted to a JS number, if not keep it as a string
		if (item['$'].length > 5) {
			console.log("not string:", item)
			stats[item['column']] = counterToNumber(item['$'])
		} else {
			console.log("string:", item)
			stats[item['column']] = item['$']
		}
	});
	return stats;
}

hclient.table('ckboyd_request_summary').row('606372021').get((error, cells) => {
	console.info(rowToMap(cells))
	console.info(cells)
})

hclient.table('ckboyd_request_crosswalk').row('BBA').get((error, value) => {
	console.info(value)
})

app.use(express.static('public'));
app.get('/summary.html',function (req, res) {
	const zipyear = req.query['zip_code_cleaned'] + req.query['year'];
	console.log(zipyear);

	hclient.table('ckboyd_request_summary').row(zipyear).get(function (err, cells) {
		const requestInfo = rowToMap(cells);
		console.log(requestInfo);

		function request_frequency(request_code) {
			let total_count = requestInfo["stats:total_count"];
			let requests = requestInfo["stats:count_" + request_code];
			if (requests == 0)
				return " - ";
			return ((requests / total_count).toFixed(3)*100).toString()+"%";
		}

		const result=[];

		for (const request_code of ['311IOC', 'AAD', 'AVN', 'BAM', 'BBA', 'BBC', 'BBD', 'BPI', 'CAFE', 'CIAC', 'CORNVEND', 'EAE', 'EAF', 'EAQ', 'ESPC', 'FAC', 'FPC', 'HFF', 'PBD', 'PBLDR', 'PBS', 'PCB', 'PCC', 'PET', 'PHB', 'PHF', 'PSL', 'QAC', 'RFC', 'SCB', 'SCS', 'SCT', 'SCX', 'SDO', 'SDP', 'SDW', 'SEC', 'SEL', 'SFC', 'SFQ', 'SGA', 'SGV', 'SIE', 'SRRC', 'SRRP', 'SWSNOREM', 'TNP', 'VBL', 'WCA3', 'WM3', 'AAE', 'AAF', 'AAI', 'BAG', 'BBK', 'BUNGALOW', 'CHECKFOR', 'CSC', 'CSF', 'CSP', 'CST', 'DBPC', 'EAB', 'EBD', 'GRAF', 'HDF', 'HFB', 'HOP', 'INR', 'JNS', 'LIQUORCO', 'LPRC', 'MWC', 'NAA', 'NOSOLCPP', 'OCC', 'ODM', 'PBE', 'PCD', 'PCE', 'PCL', 'PCL3', 'PETCO', 'RBL', 'SCC', 'SCP', 'SCQ', 'SDR', 'SED', 'SEE', 'SEF', 'SFA', 'SFB', 'SFD', 'SFK', 'SFN', 'SGG', 'SGQ', 'SHVR', 'SKA', 'SNPBLBS', 'WBJ', 'WBK', 'WBT', 'WCA', 'WCA2']) {
			const output = {};

			// the goal here was to save the actual request name to the output for each row in addition to the short_code
			// but I couldn't make it work within this loop.

			//var name_array = new Array();

			// let name = hclient.table('ckboyd_request_crosswalk').row(request_code).get(function (error, cells) {
			// 	var nameInfo = rowToMap(cells);
			// 	let name = nameInfo["name:sr_type"];
			// 	console.log("inside hclient table", name);
			// 	return name;
			// 	//name_array.push(name);
			// })

			//console.log("outside", name_array, typeof (name_array[0]));

			output["request_code"] = request_code;
			//output["request_name"] = name;
			output["count"] = requestInfo["stats:count_" + request_code];
			output["freq"] = request_frequency(request_code);
			result.push(output);
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template, {
			zip_code_cleaned: requestInfo["stats:zip_code_cleaned"],
			year: requestInfo["stats:year"],
			total_count: requestInfo["stats:total_count"],
			total_population: requestInfo["stats:total_population"],
			per_capita_requests: (requestInfo["stats:total_count"]/requestInfo["stats:total_population"]).toFixed(4),
			output: result,
		});
		res.send(html);
	});
});


// Speed layer - take in result from form and send to kafka message queue
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/log.html',function (req, res) {
	var year = req.query['year'];
	var sr_short_code = req.query['code'];
	var zip_code_cleaned = req.query['zip_code'];
	var report = {
		year : year,
		zip_code_cleaned : zip_code_cleaned,
		sr_short_code : sr_short_code,
	};

	kafkaProducer.send([{ topic: 'ckboyd_new311requests', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.send('<p style="text-align: center;font-family: Arial, sans-serif;font-size: 14px; font-weight: bold" class="basic-grey">Log Submitted!</p>');
		});
});

app.listen(port);