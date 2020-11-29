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

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = Number(item['$'])
	});
	return stats;
}

hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})

hclient.table('spertus_carriers').scan({ maxVersions: 1}, (err,rows) => {
	console.info(rows)
})

hclient.table('spertus_ontime_by_year').scan({
	filter: {type : "PrefixFilter",
		      value: "AA"},
	maxVersions: 1},
	(err, value) => {
	  console.info(value)
	})


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

app.get('/yearly-stats.html',function (req, res) {
	const year=req.query['year'];
	console.log(year);
	function processMonthRecord(monthRecord) {
		var result = { month : monthRecord['month']};
		['avg_precipitation', 'avg_snow','avg_temp','total_trips', 'average_age', 'total_rides'].forEach(col =>{
			result[col] = monthRecord[col]
		})
		result['avg_trip_duration'] = (monthRecord['trip_duration'] / monthRecord['total_trips'] *60).toFixed(1);
		result['share_subscribers'] = (monthRecord['subscribers'] / (monthRecord['subscribers'] + monthRecord['non_subscribers'])).toFixed(1)+"%";
		result['share_bus_trips'] = (monthRecord['total_bus_trips'] / monthRecord['total_rides']).toFixed(1)+"%";
		result['share_rail_trips'] = (monthRecord['total_rail_trips'] / monthRecord['total_rides']).toFixed(1)+"%";
		result['total_rides'] = monthRecord['total_rides']
		return result;
	}
	function transportInfo(cells) {
		var result = [];
		var monthRecord;
		cells.forEach(function(cell) {
			var month = Number(removePrefix(cell['key'], year + "-"))
			if(monthRecord === undefined)  {
				monthRecord = { month: month }
			} else if (monthRecord['month'] != month ) {
				result.push(processMonthRecord(monthRecord))
				monthRecord = { month: month }
			}
			monthRecord[removePrefix(cell['column'],'stat:')] = Number(cell['$']) //might have to fix the byte here
		})
		result.push(processMonthRecord(monthRecord))
		console.info(result)
		return result;
	}

	hclient.table('reid7_weather_transport_monthly').scan({
			filter: {type : "PrefixFilter",
				value: year},
			maxVersions: 1},
		(err, cells) => {
			var ti = transportInfo(cells);
			var template = filesystem.readFileSync("year-result.mustache").toString();
			var html = mustache.render(template, {
				airlineInfo : ti,
				year : year
			});
			res.send(html)

		})
});




app.get('/delays.html',function (req, res) {
    const route=req.query['origin'] + req.query['dest'];
    console.log(route);
	hclient.table('weather_delays_by_route').row(route).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)
		function weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0)
				return " - ";
			return (delays/flights).toFixed(1); /* One decimal place */
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : req.query['origin'],
			dest : req.query['dest'],
			clear_dly : weather_delay("clear"),
			fog_dly : weather_delay("fog"),
			rain_dly : weather_delay("rain"),
			snow_dly : weather_delay("snow"),
			hail_dly : weather_delay("hail"),
			thunder_dly : weather_delay("thunder"),
			tornado_dly : weather_delay("tornado")
		});
		res.send(html);
	});
});

app.get('/airline-ontime.html', function (req, res) {
	hclient.table('spertus_carriers').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("airline-ontime.mustache").toString();
		var html = mustache.render(template, {
			airlines : rows
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

app.get('/airline-ontime-delays.html',function (req, res) {
	const airline=req.query['airline'];
	console.log(airline);
	function processYearRecord(yearRecord) {
		var result = { year : yearRecord['year']};
		["all", "clear", "fog", "hail", "rain", "snow", "thunder", "tornado"].forEach(weather => {
			var flights = yearRecord[weather + '_flights']
			var ontime_flights = yearRecord[weather + "_ontime"]
			result[weather] = flights == 0 ? "-" : (100 * ontime_flights/flights).toFixed(1)+'%';
		})
		return result;
	}
	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = Number(removePrefix(cell['key'], airline))
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year ) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = Number(cell['$'])
		})
		result.push(processYearRecord(yearRecord))
		console.info(result)
		return result;
	}

	hclient.table('spertus_ontime_by_year').scan({
			filter: {type : "PrefixFilter",
				value: airline},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("ontime-result.mustache").toString();
			var html = mustache.render(template, {
				airlineInfo : ai,
				airline : airline
			});
			res.send(html)

		})
});

app.listen(port);
