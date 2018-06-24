'use strict';
const sql = require('mssql'),
	_ = require('underscore'),
	moment = require('moment'),
	rets = require('rets-client'),
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
	return await pool.request().query(sqlStr);
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

/*
*  When the program is updated to grab other than residental listings
*  then modify this function to be more dynamic based on type of Property
*/

const updateMLSInfo = async args => {
	const searchData = args.searchData;
	const allMLSNumbers = _.pluck(searchData, 'MLS_NUMBER');

	const tableMetadata = (await doQuery(args.pool, `SELECT TOP 1 * FROM ${args.search.type}`)).recordset.columns;

	const toUpdateSql = `SELECT MLS_NUMBER FROM ${args.search.type} WHERE MLS_NUMBER IN (${allMLSNumbers.join()})`;
	const toUpdate = (await doQuery(args.pool, toUpdateSql)).recordset;

	const updateBase = `UPDATE ${args.search.type} SET `;
	const insertBase = `INSERT INTO ${args.search.type} (${_.keys(tableMetadata).join()}) `;

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

const downloadImages = async args => {
	/*
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
*/
	/*
	return client.objects.getAllObjects("Property", "LargePhoto", photoSourceId, {alwaysGroupObjects: true, ObjectData: '*'})
    }).then(function (photoResults) {
      console.log("=================================");
      console.log("========  Photo Results  ========");
      console.log("=================================");
      console.log('   ~~~~~~~~~ Header Info ~~~~~~~~~');
      outputFields(photoResults.headerInfo);
      for (var i = 0; i < photoResults.objects.length; i++) {
        console.log("   -------- Photo " + (i + 1) + " --------");
        if (photoResults.objects[i].error) {
          console.log("      Error: " + photoResults.objects[i].error);
        } else {
          outputFields(photoResults.objects[i].headerInfo);
          fs.writeFileSync(
            "/tmp/photo" + (i + 1) + "." + photoResults.objects[i].headerInfo.contentType.match(/\w+\/(\w+)/i)[1],
            photoResults.objects[i].data);
        }
      }
    });
*/
};

const processPhotoData = async args => {
	console.log(args.photo.types);
};

const processData = async args => {
	try {
		const results = await getPropertyData(args);
		if (results.searchData.length > 0) {
			switch (results.search.type) {
				case 'RESI':
					await updateMLSInfo(results);
					if (results.search.getPhotos) {
						await processPhotoData(args);
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

/*
*  Main program
*/
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
	args.photo = {};
	args.photo.directory = config.photo.directory[env];
	args.photo.types = config.photo.types;

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
