'use strict';
const sql = require('mssql'),
	_ = require('underscore'),
	moment = require('moment'),
	rets = require('rets-client'),
	{ gzip, ungzip } = require('node-gzip'),
	env = process.env.NODE_ENV || 'test',
	config = require('./config.json');

/*
Residential      = "RESI"
Multifamily      = "MULTI"
Commercial       = "COMM"
Land             = "LAND"
ResidentialRooms = "ROOMS"
Units            = "UNITS"
OpenHouse        = "OPEN_HOUSES"
*/

const getPropertyData = async args => {
	return args.client.search
		.query('Property', args.search.type, args.search.dt_mod, {
			limit: args.search.limit,
			offset: args.search.offset,
		})
		.then(searchData => {
			args.searchData = searchData.results;
			args.search.count = searchData.count;
			args.search.offset += searchData.rowsReceived;
			args.search.complete = args.search.offset >= args.search.count ? true : false;
			return args;
		});
};

const doQuery = async (pool, sqlStr) => {
	console.log(sqlStr)
	return await pool.request().query(sqlStr);
};

const prepareValue = (elementType, value) => {
	value = value || 'NULL';

	if (elementType === sql.DateTime) {
		return "'" + moment(value).format('YYYYMMDD h:mm:ss a') + "'";
	}

	if (elementType === sql.VarChar && value !== null) {
		return "'" + value.replace("'", "''").trim() + "'";
	}

	/* convert bit to 0/1 */
	if (elementType === sql.Bit) {
		return value === true ? 1 : 0;
	}

	return value;
};

const updateResidental = async args => {
	const searchData = args.searchData;
	const allMLSNumbers = _.pluck(searchData, 'MLS_NUMBER');

	const tableMetadata = (await doQuery(args.pool, 'SELECT TOP 1 * FROM RESI')).recordset.columns;

	const toUpdateSql = `SELECT MLS_NUMBER FROM RESI WHERE MLS_NUMBER IN (${allMLSNumbers.join()})`;
	const toUpdate = (await doQuery(args.pool, toUpdateSql)).recordset;

	const updateBase = 'UPDATE RESI SET ';
	const insertBase = 'INSERT INTO RESI (' + _.keys(tableMetadata).join() + ') ';

	let insertMLSNumbers = _.clone(allMLSNumbers);
	args.data.updates = (await Promise
		.all(
			toUpdate.map(async existing => {
				const updateValues = [];
				const listing = _.find(searchData, { MLS_NUMBER: existing.MLS_NUMBER.toString() });
				_.keys(tableMetadata).map(key => {
					updateValues.push(`${key}=${prepareValue(tableMetadata[key].type, listing[key])}`);
					insertMLSNumbers = _.without(insertMLSNumbers, listing.MLS_NUMBER.toString());
				});
				return (await doQuery(
					args.pool,
					`${updateBase} ${updateValues.join()} WHERE MLS_NUMBER=${listing.MLS_NUMBER};`
				)).rowsAffected[0];
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			throw err;
		})) ||
		0;

	args.data.inserts = (await Promise
		.all(
			insertMLSNumbers.map(async mlsnumber => {
				const insertValues = [];
				const listing = _.find(searchData, { MLS_NUMBER: mlsnumber.toString() });
				_.keys(tableMetadata).map(key => {
					insertValues.push(prepareValue(tableMetadata[key].type, listing[key]));
				});
				return (await doQuery(
					args.pool,
					insertBase + 'VALUES(' + insertValues.join() + ')'.replace("'NULL'", 'NULL')
				)).rowsAffected[0];
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			throw err;
		})) ||
		0;
};

const processData = async args => {
	try {
		const results = await getPropertyData(args);
		if (results.searchData.length > 0) {
			switch (results.search.type) {
				case 'RESI':
					await updateResidental(results);
					if (results.getPhotos) {
						//await downloadPhotos(args.searchData);
					}
					break;
			}
		}
		delete args.searchData;
		return results.search.complete || results.search.test ? results : await processData(results);
	} catch (err) {
		args.search.error = err;
		return args;
	}
};

rets.getAutoLogoutClient(config.matrixrets, client => {
	const args = {
		client: client,
		search: config.search.residential,
		data: {
			inserts: 0,
			updates: 0,
		},
	};
	let dt_mod = moment().add(-args.search.days, 'days').format('YYYY-MM-DD') + '+';
	args.search.dt_mod = '(DT_MOD=' + dt_mod + ')';

	var results = (async args => {
		args.pool = await sql.connect(config.db[env].mssql);
		return await processData(args).then(results => {
			return results;
		});
	})(args);

	results.then(data => {
		console.log(data.search);
		console.log(data.data);
		data.pool.close();
		sql.close();
	});
});
