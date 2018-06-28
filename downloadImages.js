'use strict';
const fs = require('fs'),
	del = require('del'),
	sql = require('mssql'),
	_ = require('underscore'),
	moment = require('moment'),
	rets = require('rets-client'),
	promiseSerial = require('promise-serial'),
	env = process.env.NODE_ENV || 'test',
	config = require('./config.json');

const getImageSizeID = type => {
	switch (type) {
		case 'LargePhoto':
			return '.L';
		case 'xLargePhoto':
			return '.X';
		case 'xxLarge':
			return '.XX';
		default:
			return '';
	}
};

const downloadImages = async (listing, type, args) => {
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
				let size = getImageSizeID(type);
				const fileName = `${listing.MLS_NUMBER}${size}.${image.headerInfo.objectId}.jpg`;
				const filePath = `${args.photo.directory[env]}\\${fileName}`;
				fs.writeFileSync(filePath, image.data);
			});
			return images.length;
		})
		.catch(err => {
			if (err.httpStatus === 504 && listing.retries < 10) {
				console.log(`retry: ${currentDownload.type} - ${currentDownload.listing.MLS_NUMBER}`);
				currentDownload.listing.retries++;
				return downloadImages(currentDownload.listing, currentDownload.type, currentDownload.args);
			} else {
				args.photo.errors.push(err);
				return 0;
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

const removeFile = filePath => {
	try {
		fs.unlinkSync(filePath);
	} catch (err) {
		if (err.errno !== -4058) removeFile(filePath);
	}
};

const removeListingImages = async (listing, config) => {
	for (let type of config.types) {
		const size = getImageSizeID(type);
		for (let count = 0; count <= 99; count++) {
			const fileName = `${listing.MLS_NUMBER}${size}.${count}.jpg`;
			const filePath = `${config.directory[env]}\\${fileName}`;
			removeFile(filePath);
		}
	}
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
					const search = _.find(config.searches, { type: 'RESI' });
					const searchDate = moment().add(-search.days, 'days').format('YYYY-MM-DD');
					const listings = (await pool
						.request()
						.query(
							`SELECT MLS_NUMBER, MATRIX_UNIQUE_ID, PHOTO_COUNT FROM RESI WHERE PHOTO_MODIFIED_DATE >='${searchDate}' AND PHOTO_COUNT > 0 AND STATUS IN ('Active','Contingent','Model','Pending','Show For Backups')`
						)).recordset;

					const args = {
						client: client,
						search: search,
						photo: config.photo,
					};

					try {
						console.log(`Donwloading images for ${listings.length} listings`);

						for (let listing of listings) {
							await removeListingImages(listing, config.photo);
							args.photo.downloaded += await downloadAllImages(listing, args);
						}
						console.log(args.photo);

						pool.close();
						sql.close();

						client.logout().then(resp => {
							console.log('logged out');
							console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
						});
					} catch (err) {
						pool.close();
						sql.close();
						console.log(err);
						console.log('ERROR: in downloading');
						client.logout().then(resp => {
							console.log('logged out');
							console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
						});
					}
				})(pool);
			})
			.catch(err => {
				console.log(err);
				console.log('ERROR: in getting pool connection');
				client.logout().then(resp => {
					console.log('logged out');
					console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
				});
			});
	})
	.catch(err => {
		console.log(err);
		console.log('ERROR: in getting RETS client');
		console.log(moment().format('MM/DD/YYYY h:mm:ss a'));
	});
