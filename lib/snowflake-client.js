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


/* New code goes here */

function toSqlStmt(tableName,select,geographyField,spatialFilter,whereClause,maxRows,supportsPagination,primaryId,offset) {
    start_sql = frontSqlStmt(select, geographyField, tableName);
    end_sql = endSqlStmt(spatialFilter, geographyField, whereClause, supportsPagination,primaryId,maxRows,offset);

    return start_sql + end_sql;
}

function frontSqlStmt(select, geographyField, tableName) {
    start_sql = `select ${select},st_asgeojson(${geographyField}) as LOCATION from ${tableName} `;
    return start_sql;
}

function endSqlStmt(spatialFilter, geographyField, whereClause, supportsPagination, primaryId,maxRows,offset) {
    end_sql = '';

    if (whereClause !== '') {
        end_sql = end_sql + `where ${whereClause} `;
    }

    if (spatialFilter !== '') {
        end_sql = end_sql + `and (st_intersects(${geographyField},ST_GEOGRAPHYFROMWKT(${spatialFilter})))`;
    }

    if (supportsPagination && primaryId) {
        end_sql = end_sql + `order by ${primaryId}  `;
    }

    if (maxRows) {
        end_sql = end_sql + `limit ${maxRows.toString()} `;
    }

    if (offset) {
        end_sql = end_sql + `offset ${offset.toString()} `;
    }


    return end_sql;

}



/* New code ends here */




function toSqlStmt2(tableName,select,geographyField,spatialFilter,whereClause,maxRows){
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
 
    console.log("Printing the features");
    console.log(JSON.stringify(features));
    console.log('\n');

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

    console.log(sqlStmt);

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


async function executeCountSql({tableName, spatialFilter, geographyField, whereClause} = {}){

    sqlStmt = `select count(*) as LOCATION from ${tableName} ` + endSqlStmt(spatialFilter, geographyField, whereClause);
    console.log(sqlStmt);

    let promise = new Promise((resolve,reject)=>{
        connection.execute({
            sqlText: sqlStmt,
            complete: function(err, stmt, rows) {
              if (err) {
                reject(err);
              } else {
                // console.log("success!" + JSON.stringify(rows));  
                let features = {"type": "Feature", "geometry":null, "properties": {"count": rows[0].LOCATION}};
                // console.log("success!" + JSON.stringify(features));  
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