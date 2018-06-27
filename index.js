'use strict';
const fs = require('fs'),
	sql = require('mssql'),
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
ALTER TABLE [RESI] ALTER COLUMN [REMARKS] VARCHAR(550)
ALTER TABLE [RESI] ALTER COLUMN L_OFF_NAME VARCHAR(60)
ALTER TABLE [RESI] ALTER COLUMN L_OFF_PHONE VARCHAR(20)
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
	const insertBase = `INSERT INTO ${args.search.type} (${_.keys(tableMetadata).join()})`;

	let insertNumbers = _.clone(allNumbers);
	let sqlStr = '';

	console.log(`Database processing offset: ${args.search.offset}`);

	args.search.updates += (await Promise
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
				sqlStr = `${updateBase} ${updateValues.join()} WHERE ${args.search.key}=${listing[args.search.key]};`;
				return (await doQuery(args.pool, sqlStr)).rowsAffected[0];
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			console.log(sqlStr);
			err.sql = sqlStr;
			throw err;
		})) ||
		0;

	args.search.inserts += (await Promise
		.all(
			insertNumbers.map(async objectnumber => {
				const insertValues = [];
				const search = {};
				search[args.search.key] = objectnumber.toString();
				const listing = _.find(searchData, search);
				_.keys(tableMetadata).map(key => {
					insertValues.push(prepareValue(tableMetadata[key].type, listing[key]));
				});
				sqlStr = `${insertBase} VALUES(${insertValues.join()})`.replace("'NULL'", 'NULL');
				return (await doQuery(args.pool, sqlStr)).rowsAffected[0];
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			console.log(sqlStr);
			err.sql = sqlStr;
			throw err;
		})) ||
		0;
};

const downloadImagesAsync = async (listing, type, args) => {
	if(!_.has(listing, 'retries')) {
		listing.retries = 0;
	}
	else {
		listing.retries++;
	}
	args.photo.currentDownload = {
		listing: listing,
		type: type,
		args: args
	};

	return args.client.objects
		.getAllObjects(args.search.class, type, listing.MATRIX_UNIQUE_ID)
		.then(results => {
			const images = [];
			results.objects.forEach(obj => {
				if (obj['headerInfo'].contentType === 'image/jpeg') {
					images.push(obj);
				}
			});

			images.map(image => {
				let size = '';
				switch (type) {
					case 'LargePhoto':
						size = '.L';
						break;
					case 'xLargePhoto':
						size = '.X';
						break;
					case 'xxLarge':
						size = '.XX';
						break;
					default:
						size = '';
				}
				const fileName = `${listing.MLS_NUMBER}${size}.${image.headerInfo.objectId}.jpg`;
				const filePath = `${args.photo.directory}\\${fileName}`;
				fs.writeFileSync(filePath, image.data);
			});
			return images.length;
		})
		.catch(err => {
			if(err.httpStatus === 504 && listing.retries < 10) {
				return (async localArgs => {
					console.log(`retry: ${localArgs.type} - ${localArgs.listing.MLS_NUMBER}`);
					await downloadImages(localArgs.listing, localArgs.type, localArgs.args);
				})(args.photo.currentDownload);
			}
/*
			else if(err.replyCode === 20412 && listing.retries < 10) {
				return (async localArgs => {
					console.log(`Too many outstanding requests: ${localArgs.type} - ${localArgs.listing.MLS_NUMBER}`);
					await downloadImages(localArgs.listing, localArgs.type, localArgs.args);
				})(args.photo.currentDownload);
			}
*/
			else {
				args.photo.errors.push(err);
			}
			return(0);
		});
};

const downloadImages = (listing, type, args) => {
	if(!_.has(listing, 'retries')) {
		listing.retries = 0;
	}
	else {
		listing.retries++;
	}
	args.photo.currentDownload = {
		listing: listing,
		type: type,
		args: args
	};

	return args.client.objects
		.getAllObjects(args.search.class, type, listing.MATRIX_UNIQUE_ID)
		.then(results => {
			const images = [];
			results.objects.forEach(obj => {
				if (obj['headerInfo'].contentType === 'image/jpeg') {
					images.push(obj);
				}
			});

			images.map(image => {
				let size = '';
				switch (type) {
					case 'LargePhoto':
						size = '.L';
						break;
					case 'xLargePhoto':
						size = '.X';
						break;
					case 'xxLarge':
						size = '.XX';
						break;
					default:
						size = '';
				}
				const fileName = `${listing.MLS_NUMBER}${size}.${image.headerInfo.objectId}.jpg`;
				const filePath = `${args.photo.directory}\\${fileName}`;
				fs.writeFileSync(filePath, image.data);
			});
			return images.length;
		})
		.catch(err => {
			if(err.httpStatus === 504 && listing.retries < 10) {
				console.log(`retry: ${type} - ${listing.MLS_NUMBER}`);
				return downloadImages(listing, type, args);
			}
			else if(err.replyCode === 20412) {
				args.photo.errors.push(err.replyText);
			}
			else {
				args.photo.errors.push(err);
				return(0);
			}
		});
};
const downloadAllImages = async (listing, args) => {
	return (await Promise
		.all(
			args.photo.types.map(type => {
				return downloadImages(listing, type, args);
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

const processPhotoDataAsync = async args => {
	const listingsWithPhotos = _.filter(args.searchData, listing => {
		return listing.PHOTO_COUNT > 0 && moment(listing.PHOTO_MODIFIED_DATE) >= moment(args.search.date);
	});

	console.log(`Downloading images: offset ${args.search.offset} - found ${listingsWithPhotos.length} matching entries`);

	args.photo.downloaded += (await Promise
		.all(
			listingsWithPhotos.map(async (listing, index) => {
				return await downloadAllImages(listing, args);
			})
		)
		.then(results => {
			results[0] = results[0] || 0;
			return results.reduce((total, num) => {
				return total + num;
			});
		})
		.catch(err => {
			args.photo.errors.push(err);
		})) ||
		0;
};

const processData = async args => {
	if (!args.search.run) {
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
		args.search.errors.push(err);
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
						config.searches.map(async search => {
							search.errors = [];
							search.inserts = 0;
							search.updates = 0;
							search.date = moment().add(-search.days, 'days').format('YYYY-MM-DD');
							search.dt_mod = `(DT_MOD=${search.date}+)`;

							const args = {
								pool: pool,
								client: client,
								search: search,
							};

							if (search.getPhotos) {
								const photo = {
									errors: [],
									downloaded: 0,
									deleted: 0,
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
							if (r.search.run) {
								console.log(r.search);
							}
							if (_.has(r, 'photo')) {
								delete r.photo.currentDownload;
								console.log(r.photo);
							}
						});
						pool.close();
						sql.close();
						console.log('logging out')
						client.logout().then(resp => {
							//console.log(resp)
						});
					})
					.catch(err => {
						pool.close();
						sql.close();
						client.logout();
						throw err;
					});
			})(pool);
		})
		.catch(err => {
			client.logout();
			console.log(err);
		});
})
.catch(err => {
	if(_.has(err, 'replyCode')) {
		console.log(err.replyText);
	}
	else {
		console.log(err);
	}
});
