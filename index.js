'use strict';
const sql = require('mssql'),
	_ = require('underscore'),
	moment = require('moment'),
	rets = require('rets-client'),
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
	const allNumbers = _.pluck(searchData, args.search.key);

	const tableMetadata = (await doQuery(args.pool, `SELECT TOP 1 * FROM ${args.search.type}`)).recordset.columns;

	const toUpdateSql = `SELECT ${args.search.key} FROM ${args.search.type} WHERE ${args.search.key} IN (${allNumbers.join()})`;
	const toUpdate = (await doQuery(args.pool, toUpdateSql)).recordset;

	const updateBase = `UPDATE ${args.search.type} SET `;
	const insertBase = `INSERT INTO ${args.search.type} (${_.keys(tableMetadata).join()}) `;

	let insertNumbers = _.clone(allNumbers);

	args.search.updates = (await Promise
		.all(
			toUpdate.map(async existing => {
				const updateValues = [];
				const search = {};
				search[args.search.key] = existing[args.search.key].toString();
				const listing = _.find(searchData, search);
				_.keys(tableMetadata).map(key => {
					updateValues.push(`${key}=${prepareValue(tableMetadata[key].type, listing[key])}`);
					insertNumbers = _.without(insertNumbers, listing[args.search.key].toString());
				});
				return (await doQuery(
					args.pool,
					`${updateBase} ${updateValues.join()} WHERE ${args.search.key}=${listing[args.search.key]};`
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

	args.search.inserts = (await Promise
		.all(
			insertNumbers.map(async objectnumber => {
				const insertValues = [];
				const search = {};
				search[args.search.key] = objectnumber.toString();
				const listing = _.find(searchData, search);
				_.keys(tableMetadata).map(key => {
					insertValues.push(prepareValue(tableMetadata[key].type, listing[key]));
				});
				return (await doQuery(
					args.pool,
					insertBase + `VALUES(${insertValues.join()})`.replace("'NULL'", 'NULL')
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

const downloadImages = async (mlsnumber, type, args) => {
	/*
	const listing = _.find(args.searchData, {"MLS_NUMBER": mlsnumber });
	console.log(listing.PHOTO_COUNT + ' ' + listing.PHOTO_MODIFIED_DATE)
*/
	//console.log(args.client.objects.getObjects.toString());

	return args.client.objects
		.getAllObjects('Property', type, mlsnumber)
		.then(results => {
			//console.log(results);
		})
		.catch(err => {
			args.photo.error = err;
		});
};
const downloadAllImages = async (mlsnumber, args) => {
	return (await Promise
		.all(
			args.photo.types.map(async type => {
				return await downloadImages(mlsnumber, type, args);
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			args.photo.error = err;
		})) ||
		0;
};

const processPhotoData = async args => {
	args.photo.downloaded = (await Promise
		.all(
			args.searchData.map(async listing => {
				return await downloadAllImages(listing.MLS_NUMBER, args);
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			args.photo.error = err;
		})) ||
		0;
};

const processData = async args => {
	if(!args.search.run) {
		return args;
	}
	try {
		const results = await getData(args);
		if (results.searchData.length > 0) {
			await updateDatabase(results);
			if (results.search.getPhotos) {
				await processPhotoData(args);
			}
		}
		delete args.searchData;
		return results.search.complete || results.search.test ? results : await processData(results);
	} catch (err) {
		args.search.error = err;
		return args;
	}
};

/*
*  Main program
*/
rets.getAutoLogoutClient(config.matrixrets, client => {
	sql
		.connect(config.db[env].mssql)
		.then(pool => {
			(async pool => {
				const results = await Promise
					.all(
						config.searchs.map(async search => {
							search.error = null;
							search.inserts = 0;
							search.updates = 0;
							search.dt_mod = `(DT_MOD=${moment().add(-search.days, 'days').format('YYYY-MM-DD')}+)`;

							const args = {
								pool: pool,
								client: client,
								search: search,
							};

							if (search.getPhotos === true) {
								const photo = {
									error: null,
									downloads: 0,
									deletes: 0,
									types: config.photo.types,
									directory: config.photo.directory[env],
								};
								args.photo = photo;
							}

							return await processData(args).then(results => {
								return results;
							});
						})
					)
					.then(results => {
						results.map(r => {
							if(r.search.run) {
								console.log(r.search);
							}
							if(_.has(r, 'photo') && r.photo.error !== null) {
								console.log(r.photo.error);
							}
						});
						pool.close();
						sql.close();
					})
					.catch(err => {
						console.log(err);
						pool.close();
						sql.close();
					});
			})(pool);
		})
		.catch(err => {
			console.log(err);
		});
});
