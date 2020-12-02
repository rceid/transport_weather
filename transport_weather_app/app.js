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
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

app.use(express.static('public'));
app.get('/home.html',function (req, res) {
	var template = filesystem.readFileSync("home.mustache").toString();
	var html = mustache.render(template)
	res.send(html)
});

app.get('/divvy-cta-yearly.html', function (req, res) {
	hclient.table('reid7_yrs').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("divvy-cta-yearly.mustache").toString();
		var html = mustache.render(template, {
			years : rows
		});
		res.send(html)
	})
});

app.get('/snow.html', function (req, res) {
	hclient.table('reid7_snow_categories').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("snow-category.mustache").toString();
		var html = mustache.render(template, {
			categories : rows
		});
		res.send(html)
	})
});

app.get('/precipitation.html', function (req, res) {
	hclient.table('reid7_precip_categories').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("precip-category.mustache").toString();
		var html = mustache.render(template, {
			categories : rows
		});
		res.send(html)
	})
});


app.get('/yearly-stats.html',function (req, res) {
	const year=req.query['year'];
	function processMonthRecord(monthRecord) {
		try {
			var result = {month: monthRecord['month']};
		} catch(err) {
			var template = filesystem.readFileSync("error.mustache").toString();
			var html = mustache.render(template, {
				key : year
			});
			res.send(html)
		}
		['avg_precipitation', 'avg_snow','avg_temp', 'average_age'].forEach(col =>{
			result[col] = monthRecord[col]
		})
		result['total_trips'] = monthRecord['total_trips'].toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
		result['avg_trip_duration'] = (monthRecord['trip_duration'] / (monthRecord['total_trips'] *60)).toFixed(1);
		result['share_subscribers'] = ((monthRecord['subscibers'] /
			(monthRecord['subscibers'] + monthRecord['non_subscribers']))*100).toFixed(0)+"%";
		result['share_bus_trips'] = ((monthRecord['total_bus_trips'] / monthRecord['total_rides'])*100).toFixed(0)+"%";
		result['share_rail_trips'] = ((monthRecord['total_rail_trips'] / monthRecord['total_rides']*100)).toFixed(0)+"%";
		result['total_rides'] = monthRecord['total_rides'].toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
		return result;
	}

	function transportInfo(cells) {
		var result = [];
		var monthRecord;
		cells.forEach(function(cell) {
			var month = removePrefix(cell['key'], year + "-")
			if(monthRecord === undefined)  {
				monthRecord = { month: month }
			} else if (monthRecord['month'] != month ) {
				result.push(processMonthRecord(monthRecord))
				monthRecord = { month: month }
			}
			if (cell['column'] != 'stat:month') {
				try {
					monthRecord[removePrefix(cell['column'], 'stat:')] = counterToNumber(cell['$'])
				} catch (err) {
					monthRecord[removePrefix(cell['column'], 'stat:')] = cell['$']
				}
			}

		})
		result.push(processMonthRecord(monthRecord))
		return result;
	}

	hclient.table('reid7_transport_weather_monthly').scan({
			filter: {type : "PrefixFilter",
				value: year},
			maxVersions: 1},
		(err, cells) => {
			var ti = transportInfo(cells);
			var template = filesystem.readFileSync("year-result.mustache").toString();
			var html = mustache.render(template, {
				transportInfo : ti,
				year : year
			});
			res.send(html)

		})
});


app.get('/snow-cat.html',function (req, res) {
	const weather=req.query['cat'];
	function processWeatherRecord(weatherRecord) {
		try {
			var result = {date: weatherRecord['date']};
		} catch(err) {
			var template = filesystem.readFileSync("error.mustache").toString();
			var html = mustache.render(template, {
				key : weather
			});
			res.send(html)
		}
		Object.keys(weatherRecord).forEach(col =>{
			result[col] = weatherRecord[col]
		})
		result['avg_trip_duration'] = (weatherRecord['trip_duration'] / (weatherRecord['total_trips'] *60)).toFixed(1);
		result['share_subscribers'] = (weatherRecord['subscibers'] /
			(weatherRecord['subscibers'] + weatherRecord['non_subscribers'])).toFixed(2)*100+"%";
		result['share_bus_trips'] = ((weatherRecord['total_bus_trips'] / weatherRecord['total_rides'])*100).toFixed(0)+"%";
		result['share_rail_trips'] = ((weatherRecord['total_rail_trips'] / weatherRecord['total_rides']*100)).toFixed(0)+"%";
		result['pct_diff_d_trips'] = (((result['total_trips'] - result['total_trips_avg_mo']) / result['total_trips_avg_mo'])*100).toFixed(0)+"%"
		result['trip_duration_avg_mo'] = result['trip_duration_avg_mo']/ 60
		result['pct_diff_duration'] = (((result['avg_trip_duration'] - (result['trip_duration_avg_mo'])) / (result['trip_duration_avg_mo']))*100).toFixed(0)+"%"
		result['pct_diff_bus'] = (((result['total_bus_trips'] - result['total_bus_avg_mo']) / result['total_bus_avg_mo'])*100).toFixed(0)+"%"
		result['pct_diff_rail'] = (((result['total_rail_trips'] - result['total_rail_avg_mo']) / result['total_rail_avg_mo'])*100).toFixed(0)+"%"
		result['pct_diff_cta_trips'] = (((result['total_rides'] - result['total_rides_avg_mo']) / result['total_rides_avg_mo'])*100).toFixed(0)+"%"
		result['total_rides'] = weatherRecord['total_rides'].toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
		result['total_trips'] = weatherRecord['total_trips'].toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
		return result;
	}

	function weatherInfo(cells) {
		var result = [];
		var weatherRecord;
		cells.forEach(function(cell) {
			var date = removePrefix(cell['key'], weather + "-")
			if(weatherRecord === undefined)  {
				weatherRecord = { date: date }
			} else if (weatherRecord['date'] != date ) {
				result.push(processWeatherRecord(weatherRecord))
				weatherRecord = { date: date }
			}
			try {
				weatherRecord[removePrefix(cell['column'], 'stat:')] = counterToNumber(cell['$'])
			} catch (err) {
				weatherRecord[removePrefix(cell['column'], 'stat:')] = cell['$']
			}

		})
		result.push(processWeatherRecord(weatherRecord))
		return result;
	}

	hclient.table('reid7_daily_snow').scan({
			filter: {type : "PrefixFilter",
				value: weather},
			maxVersions: 1},
		(err, cells) => {
			var wi = weatherInfo(cells);
			var template = filesystem.readFileSync("weather-result.mustache").toString();
			var html = mustache.render(template, {
				weatherInfo : wi,
				key : weather,
				value: "Snow"
			});
			res.send(html)

		})
});


app.get('/precip-cat.html',function (req, res) {
	const weather=req.query['cat'];
	function processWeatherRecord(weatherRecord) {
		try {
			var result = {date: weatherRecord['date']};
		} catch(err) {
			var template = filesystem.readFileSync("error.mustache").toString();
			var html = mustache.render(template, {
				key : weather
			});
			res.send(html)
		}
		Object.keys(weatherRecord).forEach(col =>{
			result[col] = weatherRecord[col]
		})
		result['avg_trip_duration'] = (weatherRecord['trip_duration'] / (weatherRecord['total_trips'] *60)).toFixed(1);
		result['share_subscribers'] = (weatherRecord['subscibers'] /
			(weatherRecord['subscibers'] + weatherRecord['non_subscribers'])).toFixed(2)*100+"%";
		result['share_bus_trips'] = ((weatherRecord['total_bus_trips'] / weatherRecord['total_rides'])*100).toFixed(0)+"%";
		result['share_rail_trips'] = ((weatherRecord['total_rail_trips'] / weatherRecord['total_rides']*100)).toFixed(0)+"%";
		result['pct_diff_d_trips'] = (((result['total_trips'] - result['total_trips_avg_mo']) / result['total_trips_avg_mo'])*100).toFixed(0)+"%"
		result['trip_duration_avg_mo'] = result['trip_duration_avg_mo']/ 60
		result['pct_diff_duration'] = (((result['avg_trip_duration'] - (result['trip_duration_avg_mo'])) / (result['trip_duration_avg_mo']))*100).toFixed(0)+"%"
		result['pct_diff_bus'] = (((result['total_bus_trips'] - result['total_bus_avg_mo']) / result['total_bus_avg_mo'])*100).toFixed(0)+"%"
		result['pct_diff_rail'] = (((result['total_rail_trips'] - result['total_rail_avg_mo']) / result['total_rail_avg_mo'])*100).toFixed(0)+"%"
		result['pct_diff_cta_trips'] = (((result['total_rides'] - result['total_rides_avg_mo']) / result['total_rides_avg_mo'])*100).toFixed(0)+"%"
		result['total_rides'] = weatherRecord['total_rides'].toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
		result['total_trips'] = weatherRecord['total_trips'].toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
		return result;
	}

	function weatherInfo(cells) {
		var result = [];
		var weatherRecord;
		cells.forEach(function(cell) {
			var date = removePrefix(cell['key'], weather + "-")
			if(weatherRecord === undefined)  {
				weatherRecord = { date: date }
			} else if (weatherRecord['date'] != date ) {
				result.push(processWeatherRecord(weatherRecord))
				weatherRecord = { date: date }
			}
			try {
				weatherRecord[removePrefix(cell['column'], 'stat:')] = counterToNumber(cell['$'])
			} catch (err) {
				weatherRecord[removePrefix(cell['column'], 'stat:')] = cell['$']
			}

		})
		result.push(processWeatherRecord(weatherRecord))
		return result;
	}

	hclient.table('reid7_daily_precip').scan({
			filter: {type : "PrefixFilter",
				value: weather},
			maxVersions: 1},
		(err, cells) => {
			var wi = weatherInfo(cells);
			var template = filesystem.readFileSync("weather-result.mustache").toString();
			var html = mustache.render(template, {
				weatherInfo : wi,
				key : weather,
				value: "Precipitation"
			});
			res.send(html)

		})
});

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}


/* Send simulated Divvy trips to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);


app.get('/divvy.html',function (req, res) {
	var year = req.query['year'];
	var month = req.query['month'];
	var yob = req.query['yob'];
	var trip_duration = req.query['duration'];
	var subscriber = (req.query['subscriber']) ? 1 : 0;
	var report = {
		year : year,
		month : month,
		yob : yob,
		trip_duration: trip_duration,
		subscriber : subscriber
	};

	kafkaProducer.send([{ topic: 'reid7_transport_weather', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log("Kafka Error: " + err)
			console.log(data);
			console.log(report);
			res.redirect('submit-divvy-trip.html');
		});
});


app.listen(port);
