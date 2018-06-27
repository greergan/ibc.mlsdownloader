'use strict';
const fs = require('fs'),
	sql = require('mssql'),
	_ = require('underscore'),
	moment = require('moment'),
	rets = require('rets-client'),
	promiseSerial = require('promise-serial'),
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
*/

const downloadImages = (listing, type, args) => {
	if (!_.has(listing, 'retries')) {
		listing.retries = 0;
	}
	const currentDownload = {
		listing: listing,
		type: type,
		args: args,
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
				const filePath = `${args.photo.directory[env]}\\${fileName}`;
				fs.writeFileSync(filePath, image.data);
			});
			args.photo.downloaded += images.length;
			return args;
		})
		.catch(err => {
			if (err.httpStatus === 504 && listing.retries < 10) {
				console.log(`retry: ${currentDownload.type} - ${currentDownload.listing.MLS_NUMBER}`);
				currentDownload.listing.retries++;
				return downloadImages(currentDownload.listing, currentDownload.type, currentDownload.args);
			} else {
				args.photo.errors.push(err);
			}
			return args;
		});
};

/*
*  Main program
*/
console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
rets
	.getAutoLogoutClient(config.matrixrets, client => {
		sql
			.connect(config.db[env].mssql)
			.then(pool => {
				(async pool => {
					const search = _.where(config.searches, { type: 'RESI' })[0];
					const searchDate = moment().add(-search.days, 'days').format('YYYY-MM-DD');
					const listings = (await pool
						.request()
						.query(
							`SELECT MLS_NUMBER, MATRIX_UNIQUE_ID FROM RESI WHERE PHOTO_MODIFIED_DATE >='${searchDate}' AND PHOTO_COUNT > 0`
						)).recordset;

					const args = {
						client: client,
						search: search,
						photo: config.photo,
					};
/*
					const promiseSerial = funcs =>
					  funcs.reduce((promise, func) =>
					    promise.then(result =>
					      func().then(Array.prototype.concat.bind(result))),
					      Promise.resolve([]))
*/
const l = [];
l[0] = listings[0];
l[1] = listings[1];
const types = []
types[0] = config.photo.types[0];
const p = [];
					try {
						await(async () => {
							l.forEach(async listing => {
								types.forEach(async type => {
									setTimeout(() => {
										downloadImages(listing, type, args).then(r => console.log(r.photo))
									}, 1000);
								});
							});
						});
						pool.close();
						sql.close();

						console.log('logging out');
						client.logout().then(resp => {
							console.log('logged out');
							console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
						});
					} catch (err) {
						console.log(err);
					}
				})(pool);
			})
			.catch(err => {
				console.log(err);
				console.log('logging out');
				client.logout().then(resp => {
					console.log('logged out');
				});
			});
	})
	.catch(err => {
		console.log(err);
	});
