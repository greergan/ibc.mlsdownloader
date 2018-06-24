'use strict'
const sql = require('mssql'),
      _ = require('underscore'),
      moment = require('moment'),
      rets = require('rets-client'),
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
      return args.client.search.query("Property", args.search.type, args.search.dt_mod,
                                      {limit: args.search.limit, offset: args.search.offset})
        .then(searchData => {
          args.searchData = searchData.results;
          args.search.count = searchData.count;
          args.search.offset += searchData.rowsReceived;
          args.search.complete = (args.search.offset >= args.search.count) ? true: false;
          return args;
      });    
}

const doQuery = async (pool, sqlStr) => {
    return await pool.request().query(sqlStr);
};

const updateResidental = async args => {
    const searchData = args.searchData;
    const mlsNumbers = _.pluck(searchData, "MLS_NUMBER").join();
    let toUpdateSql = `SELECT * FROM RESI WHERE MLS_NUMBER IN (${mlsNumbers})`;
    const toUpdate = (await doQuery(args.pool, toUpdateSql)).recordset;
    const updateBase = "UPDATE RESI SET ";

    const results = await Promise.all(toUpdate.map(async (listing) => {
        const updateValues = [];
        _.keys(listing).map(key => {
            if(typeof listing[key] === 'object') {
                listing[key] = moment(listing[key]).format("YYYYMMDD h:mm:ss a");
            }
            if(typeof listing[key] === 'string') {
                listing[key] = listing[key].replace("'", "''");
            }
            listing[key] = (typeof listing[key] === 'string') ? `'${listing[key].trim()}'` : listing[key];
            if(listing[key] === true) {
                listing[key] = 1;
            }
            else if(listing[key] === false) {
                listing[key] = 0;
            }
            updateValues.push(`${key}=${listing[key]}`);
        });
        const updateSql = `${updateBase} ${updateValues.join()} WHERE MLS_NUMBER=${listing.MLS_NUMBER};`;
        return updateSql;
    })).then(results => {
        return results;
    }).catch(err => {
        console.log(err)
    });

    (async pool => {
        return await Promise.all(results.map(async sqlStr => {
            return await doQuery(pool, sqlStr);
        })).then(results => {
            console.log(results);
        }).catch(err => {
            console.log(err)
        });
    })(args.pool);
}

const processData = async (args) => {
    try {
        const results = await getPropertyData(args);
        if(results.searchData.length > 0) {
            switch(results.search.type) {
                case 'RESI':
                    await updateResidental(results);
                    if(results.getPhotos) {
                        //await downloadPhotos(args.searchData);
                    }
                    break;
            }
        }
        delete args.searchData;
        return (results.search.complete || results.search.test) ? results : await processData(results);
    }
    catch(err) {
        args.search.error = err;
        return(args);
    }
}

rets.getAutoLogoutClient(config.matrixrets, client => {
    const args = {
        "client": client,
        "search": config.search.residential
    };
    let dt_mod = moment().add(-args.search.days, 'days').format("YYYY-MM-DD") + "+";
    args.search.dt_mod = "(DT_MOD=" + dt_mod + ")";

    var results = (async args => {
        args.pool = await sql.connect(config.mssql);
        return await processData(args).then(results => {
            return results;
        });
    })(args);

    results.then(data => {
        console.log(data.search);
        data.pool.close();
        sql.close();
    });
});
