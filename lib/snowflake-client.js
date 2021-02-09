const snowflake = require('snowflake-sdk');
var connection;

function toSqlStmt(tableName,select,geometryField,spatialFilter,whereClause){
    var sql = '';
    if(spatialFilter !== ''){
        if(whereClause !== ''){
            sql = `select ${select},st_asgeojson(${geometryField}) as LOCATION 
                    from ${tableName} 
                    where (${whereClause}) and (st_intersects(${geometryField},ST_GEOGRAPHYFROMWKT(${spatialFilter})))`;
        }
        else {
            sql = `select ${select},st_asgeojson(${geometryField}) as LOCATION 
                    from ${tableName} 
                    where st_intersects(${geometryField},ST_GEOGRAPHYFROMWKT(${spatialFilter}))`;
        }
    }
    else{
        if(whereClause !== ''){
            sql = `select ${select},st_asgeojson(${geometryField}) as LOCATION from ${tableName} where ${whereClause}`
        }
        else{
            sql = `select ${select},st_asgeojson(${geometryField}) as LOCATION from ${tableName}`
        }
    }
    return sql;
}

function rowToProperties(select,row){
    let fields = select.split(",");
    let props = {}
    for (f = 0; f < fields.length; f++) {
        let name = fields[f]
        props[name] = row[name];
    }
    return props;
}

function rowsToGeoJson(select,rows){
    let features = [];
    var i;
    for (i = 0; i < rows.length; i++) {
        features.push({
            "type": "Feature",
            "geometry":rows[i].LOCATION,
            "properties": rowToProperties(select,rows[i])
        })
    }
    return features;
}

async function connect(snowflakeAccount,snowflakeWarehouse,snowflakeUser,snowflakePassword){
    let promise = new Promise((resolve,reject)=>{
        connection = snowflake.createConnection({
            account: snowflakeAccount,
            username: snowflakeUser,
            password: snowflakePassword,
            warehouse:snowflakeWarehouse
            }
        );
        
        connection.connect( 
            function(err, conn) {
                if (err) {
                    reject(new Error('Unable to connect to Snowflake: ' + err.message))
                } 
                else {
                    resolve(conn.getId())

                }
            }
        );
    });

    let result = await promise;
    return result;
}

async function executeSql(tableName,select,geometryField,spatialFilter,whereClause){
    let promise = new Promise((resolve,reject)=>{
        connection.execute({
            sqlText: toSqlStmt(tableName,select,geometryField,spatialFilter,whereClause),
            complete: function(err, stmt, rows) {
              if (err) {
                reject(err);
              } else {
                let features = rowsToGeoJson(select,rows);
                resolve(features);
              }
            }
        });
    });

    let result = await promise;
    return result;
    
}

module.exports.connect = connect;
module.exports.query = executeSql;