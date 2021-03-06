'use strict';
const sql = require('mssql'),
	_ = require('lodash'),
	moment = require('moment'),
	rets = require('rets-client'),
	mailer = require('nodemailer'),
	env = process.env.NODE_ENV || 'test',
	config = require('./config.json');

/*
Residential      = "PROPERTY.RESI"
Multifamily      = "PROPERTY.MULTI"
Commercial       = "PROPERTY.COMM"
Land             = "PROPERTY.LAND"
ResidentialRooms = "MISC.ROOMS"
Units            = "MISC.UNITS"
OpenHouse        = "MISC.OPEN_HOUSES"

alter table [MLSRets].[dbo].[OPEN_HOUSES] add  MATRIX_UNIQUE_ID bigint

ALTER TABLE [RESI] ALTER COLUMN [REMARKS] VARCHAR(550)
ALTER TABLE [RESI] ALTER COLUMN L_OFF_NAME VARCHAR(60)
ALTER TABLE [RESI] ALTER COLUMN L_OFF_PHONE VARCHAR(20)

ALTER TABLE [LAND] ALTER COLUMN [REMARKS] VARCHAR(550)
ALTER TABLE [LAND] ALTER COLUMN L_OFF_NAME VARCHAR(60)
ALTER TABLE [LAND] ALTER COLUMN L_OFF_PHONE VARCHAR(20)

ALTER TABLE [MULTI] ALTER COLUMN [REMARKS] VARCHAR(550)
ALTER TABLE [MULTI] ALTER COLUMN L_OFF_NAME VARCHAR(60)
ALTER TABLE [MULTI] ALTER COLUMN L_OFF_PHONE VARCHAR(20)

ALTER TABLE [COMM] ALTER COLUMN [REMARKS] VARCHAR(550)
ALTER TABLE [COMM] ALTER COLUMN L_OFF_NAME VARCHAR(60)
ALTER TABLE [COMM] ALTER COLUMN L_OFF_PHONE VARCHAR(20)
*/

const doQuery = async (pool, sqlStr) => {
	return await pool.request().query(sqlStr);
};

const getData = async args => {
	return args.client.search
		.query(args.search.class, args.search.type, args.search.dt_mod, {
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

const prepareValue = (elementType, value) => {
	value = value || 'NULL';

	if (elementType === sql.DateTime && value !== 'NULL') {
		return "'" + moment(value).format('YYYYMMDD h:mm:ss a') + "'";
	} else if (elementType === sql.VarChar && value !== 'NULL') {
		return "'" + value.replace(/'/gi, "''").trim() + "'";
	} else if (elementType === sql.Bit) {
		return value === true ? 1 : 0;
	}
	return value;
};

const updateDatabase = async args => {
	const searchData = args.searchData;
	const allNumbers = _.map(searchData, args.search.key);
	const tableMetadata = (await doQuery(args.pool, `SELECT TOP 1 * FROM ${args.search.type}`)).recordset.columns;
	const toUpdateSql = `SELECT ${args.search.key} FROM ${args.search.type} WHERE ${args.search.key} IN (${allNumbers.join()})`;
	const toUpdate = (await doQuery(args.pool, toUpdateSql)).recordset;
	const updateBase = `UPDATE ${args.search.type} SET `;
	const insertBase = `INSERT INTO ${args.search.type} (${_.keys(tableMetadata).join()})`;

	let insertNumbers = _.clone(allNumbers);
	let sqlStr = '';

	for (let existing of toUpdate) {
		const updateValues = [];
		const search = {};
		search[args.search.key] = existing[args.search.key].toString();
		const listing = _.find(searchData, search);
		_.keys(tableMetadata).map(key => {
			updateValues.push(`${key}=${prepareValue(tableMetadata[key].type, listing[key])}`);
			insertNumbers = _.without(insertNumbers, listing[args.search.key].toString());
		});
		sqlStr = `${updateBase} ${updateValues.join()} WHERE ${args.search.key}=${listing[args.search.key]};`;

		try {
			args.search.updates += (await doQuery(args.pool, sqlStr)).rowsAffected[0];
		} catch (err) {
			err.sql = sqlStr;
			args.search.errors.push(err);
		}
	}

	for (let objectnumber of insertNumbers) {
		const insertValues = [];
		const search = {};
		search[args.search.key] = objectnumber.toString();
		const listing = _.find(searchData, search);
		_.keys(tableMetadata).map(key => {
			insertValues.push(prepareValue(tableMetadata[key].type, listing[key]));
		});
		sqlStr = `${insertBase} VALUES(${insertValues.join()})`.replace("'NULL'", 'NULL');
		try {
			args.search.inserts += (await doQuery(args.pool, sqlStr)).rowsAffected[0];
		} catch (err) {
			err.sql = sqlStr;
			args.search.errors.push(err);
		}
	}
};

const processData = async args => {
	if (!args.search.run) {
		return args;
	}
	try {
		console.log(`Processing ${args.search.type} offset: ${args.search.offset}`);
		const results = await getData(args);
		if (results.searchData.length > 0) {
			await updateDatabase(results);
		}
		delete args.searchData;
		return results.search.complete || results.search.test ? results : await processData(results);
	} catch (err) {
		args.search.errors.push(err);
		return args;
	}
};

/*
*  Main program
*/
const start = moment().format('MM/DD/YYYY h:mm:ss a');
console.log(start);
rets
	.getAutoLogoutClient(config.matrixrets, client => {
		sql
			.connect(config.db[env].mssql)
			.then(pool => {
				(async pool => {
					for (let search of config.searches) {
						search.errors = [];
						search.inserts = 0;
						search.updates = 0;
						search.date = moment().add(-search.days, 'days').format('YYYY-MM-DD');
						search.dt_mod = `(DT_MOD=${search.date}+)`;

						await processData({
							pool: pool,
							client: client,
							search: search,
						}).then(results => {
							if (results.search.run) {
								const end = moment().format('MM/DD/YYYY h:mm:ss a');
								const transporter = mailer.createTransport(config.email.smtp);
								const options = config.email.basics;
								options.subject = options.subjectBase + ` - ${results.search.type}`;
								options.text = `${JSON.stringify(results.search)}\n${start}\n${end}`;
								transporter.sendMail(options, (err, info) => {
									if (err) {
										console.log('ERROR sending mail');
										console.log(results.search);
										console.log(end);
									}
								});
							}
						});
					}
					pool.close();
					sql.close();
					client.logout().then(resp => {
						console.log('logged out');
						console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
					});
				})(pool);
			})
			.catch(err => {
				console.log(err);
				console.log('ERROR');
				client.logout().then(resp => {
					console.log('logged out');
					console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
				});
			});
	})
	.catch(err => {
		console.log(err);
		console.log('ERROR');
		console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
	});
