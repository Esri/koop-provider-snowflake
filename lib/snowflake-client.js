/* Copyright 2021 Esri
*
* Licensed under the Apache License Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

const snowflake = require('snowflake-sdk');
var connection;

// generates the entire sql statment
function toSqlStmt(tableName,select,geographyField,spatialFilter,whereClause,maxRows,supportsPagination,primaryId,offset) {
    selectSql = selectSqlStmt(select, geographyField, tableName);

    filterSql = "";
    if (spatialFilter || whereClause) {
        filterSql = filterSqlStmt(spatialFilter, geographyField, whereClause);
    }
    
    limitOffsetSql = limitOffsetSqlStmt(supportsPagination,primaryId,maxRows,offset);

    return selectSql + filterSql + limitOffsetSql;
}

// generates sql to select data from the specified table
function selectSqlStmt(select, geographyField, tableName) {
    selectSql = `select ${select},st_asgeojson(${geographyField}) as LOCATION from ${tableName} `;
    return selectSql;
}


// generates sql to filter out rows from the selectSqlStmt
function filterSqlStmt(spatialFilter, geographyField, whereClause,) {
    filterSql = 'where ';

    if (whereClause) {
        filterSql = filterSql + `${whereClause} `;

        if (spatialFilter) {
            filterSql = filterSql + "and ";
        }
    }

    if (spatialFilter) {
        filterSql = filterSql + `(st_intersects(${geographyField},ST_GEOGRAPHYFROMWKT(${spatialFilter})))`;
    }

    return filterSql;
}

// generates sql to work the limit and offset keywords in sql
function limitOffsetSqlStmt(supportsPagination, primaryId, maxRows,offset) {
    limitOffsetSql = '';
    if (supportsPagination) {
        if (primaryId) {
            filterSql = filterSql + `order by ${primaryId}  `;
        }
        else {
            let IdErrorMessage = 'No primary ID specified for pagination to work';
            console.log(IdErrorMessage);
            throw IdErrorMessage;
        }
    }

    if (maxRows) {
        filterSql = filterSql + `limit ${maxRows.toString()} `;
    }

    if (offset) {
        if (supportsPagination) {
            filterSql = filterSql + `offset ${offset.toString()} `;
        }
        else {
            let offsetWarningMessage = 'Warning: resultOffset not valid unless supportsPagination is true';
            console.log(offsetWarningMessage);
        }
    }

    return limitOffsetSql;
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

    let result = await promise.catch(error => {
        throw error;
    });
    return result;
}

// runs when the returnCountOnly query param is false
async function executeQuerySql({tableName,select,geographyField,spatialFilter,whereClause,maxRows,supportsPagination,primaryId,offset} = {}){
    sqlStmt = toSqlStmt(tableName,
        select,
        geographyField,
        spatialFilter,
        whereClause,
        maxRows,
        supportsPagination,
        primaryId,
        offset);

    let promise = new Promise((resolve,reject)=>{
        connection.execute({
            sqlText: sqlStmt,
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

    let result = await promise.catch(error => {
        console.log("Unable to execute SQL Query: " + error);
        throw error;
    });
    return result;
    
}

// runs when the returnCountOnly query param is true
async function executeCountSql({tableName, spatialFilter, geographyField, whereClause} = {}){
    sqlStmt = `select count(*) as LOCATION from ${tableName} ` + filterSqlStmt(spatialFilter, geographyField, whereClause);
    
    let promise = new Promise((resolve,reject)=>{
        connection.execute({
            sqlText: sqlStmt,
            complete: function(err, stmt, rows) {
              if (err) {
                reject(err);
              } else {
                let features = rows[0].LOCATION;
                resolve(features);
              }
            }
        });
    });

    let result = await promise.catch(error => {
        console.log("Unable to execute SQL Query: " + error);
        throw error;
    });
    return result;
    
}

module.exports.connect = connect;
module.exports.query = executeQuerySql;
module.exports.count = executeCountSql;
