const snowflake = require('snowflake-sdk');
var connection;

function toSqlStmt(tableName,select,geographyField,spatialFilter,whereClause,maxRows){
    var sql = '';
    let selectTop = maxRows.toString();
    if(spatialFilter !== ''){
        if(whereClause !== ''){
            sql = `select top ${selectTop} ${select},st_asgeojson(${geographyField}) as LOCATION 
                    from ${tableName} 
                    where (${whereClause}) and (st_intersects(${geographyField},ST_GEOGRAPHYFROMWKT(${spatialFilter})))`;
        }
        else {
            sql = `select top ${selectTop} ${select},st_asgeojson(${geographyField}) as LOCATION 
                    from ${tableName} 
                    where st_intersects(${geographyField},ST_GEOGRAPHYFROMWKT(${spatialFilter}))`;
        }
    }
    else{
        if(whereClause !== ''){
            sql = `select top ${selectTop} ${select},st_asgeojson(${geographyField}) as LOCATION from ${tableName} where ${whereClause}`
        }
        else{
            sql = `select top ${selectTop} ${select},st_asgeojson(${geographyField}) as LOCATION from ${tableName}`
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

async function executeSql(tableName,select,geographyField,spatialFilter,whereClause,maxRows){
    let promise = new Promise((resolve,reject)=>{
        connection.execute({
            sqlText: toSqlStmt(tableName,select,geographyField,spatialFilter,whereClause,maxRows),
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