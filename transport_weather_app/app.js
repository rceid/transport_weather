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
		var template = filesystem.readFileSync("weather-category.mustache").toString();
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
		result['share_subscribers'] = (monthRecord['subscibers'] /
			(monthRecord['subscibers'] + monthRecord['non_subscribers'])).toFixed(2)*100+"%";
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


app.get('/weather-cat.html',function (req, res) {
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
		console.log(result)
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

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
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
