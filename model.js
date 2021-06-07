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

const config = require('config');
const snowflake = require('./lib/snowflake-client');
const serviceDefs = config.koopProviderSnowflake.serviceDefinitions

var isConnectionAlive = false;

/**
 * Model constructor
 */
function Model () {
    //Snowflake connection 
    snowflake.connect(config.koopProviderSnowflake.snowflakeAccount,
                    config.koopProviderSnowflake.snowflakeWarehouse,
                    config.koopProviderSnowflake.snowflakeUser,
                    config.koopProviderSnowflake.snowflakePassword).then((result)=>{
                        console.log('Successfully connected to Snowflake with connection id ' + result);
                        isConnectionAlive = true;
                    },(error)=>{
                        console.log(error.message)
                        isConnectionAlive = false;
                    });;
}

function polygonToWkt(geoJson){
    var wkt = "'POLYGON((";
    var r;
    for (r= 0; r < geoJson.geometry.coordinates.length; r++) {
        var ring = geoJson.geometry.coordinates[r];
        var i;
        for (i= 0; i < ring.length; i++) {
            var pt = ring[i];
            wkt += pt[0].toString() + ' ' + pt[1].toString() + ',';
        }
        wkt = wkt.slice(0, -1);
    }
    wkt +=  "))'";
    return wkt;
}
function projectGeoJsonGeometry(geoJson){
    return Turf.toWgs84(geoJson);

}
function esriGeometryToGeoJson(geometry,type){
    //@todo - handle other types
    switch (type){
        case 'esriGeometryEnvelope':
            let xmax = geometry.xmax;
            let xmin = geometry.xmin;
            let ymax = geometry.ymax;
            let ymin = geometry.ymin;
            
            return Turf.polygon([[[xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin], [xmax, ymin]]]);
    }

}
function requestToSpatialFilter(req) {
    if (typeof req.query.geometry !== 'undefined') {
        let reqGeometry = JSON.parse(req.query.geometry);
        var geoJson = esriGeometryToGeoJson(reqGeometry,req.query.geometryType);
        if(req.query.inSR == "102100"){
            let projected = projectGeoJsonGeometry(geoJson);
            return polygonToWkt(projected)
        }
        else if(req.query.inSR == "4326"){
            return polygonToWkt(geoJson)
        }
    }
    else {
        return '';
    }
}
function requestToWhereClause(req){
    if (typeof req.query.wheres !== 'undefined' && req.query.wheres !== '') {
        return req.query.wheres;
    }
    else{
        return '';
    }
}

/**
 * Fetch data from Snowflake. Pass result or error to callback.
 * @param {object} req express request object
 * @param {function} callback
 */
Model.prototype.getData = async function (req, callback) {
    if (!isConnectionAlive) {
        console.log("Connection to Snowflake is not alive. Please restart server.")
        let err = new Error(JSON.stringify({code:503,message:"Service unavailable"}))
        return callback(err)
    }
``
    // console.log(JSON.stringify(req.query));
    // console.log(JSON.stringify(req.params));

    if (req.path.endsWith('query')){
        let name = req.params.id;
        let idx = req.params.layer;

        let isCountOnly = req.query.returnCountOnly;
        let supportsPagination = req.query.supportsPagination;
        let offset = req.query.resultOffset

        if(idx !== null && name !== null){
            let sd = serviceDefs.get(name)
            let wkt = requestToSpatialFilter(req);
            let where = requestToWhereClause(req);

            
            let snowflake_stmt;

            if (isCountOnly) {
                snowflake_stmt = snowflake.count({"tableName": sd[idx].tableName, 
                                                  "spatialFilter": wkt,
                                                  "geographyField": sd[idx].geographyField, 
                                                  "whereClause": where
                                                });
            } 
            else {
                snowflake_stmt = snowflake.query({"tableName": sd[idx].tableName, 
                                                  "select": sd[idx].selectFields,
                                                  "geographyField": sd[idx].geographyField, 
                                                  "spatialFilter":wkt, 
                                                  "whereClause": where, 
                                                  "maxRows": sd[idx].maxReturnCount,
                                                  "supportsPagination": supportsPagination,
                                                  "primaryId": sd[idx].primaryId, 
                                                  "offset": offset
                                                });
            }

            snowflake_stmt.then((result)=>{

                // console.log(JSON.stringify(result));

                let data = {
                    type: 'FeatureCollection',
                    features: result,
                    metadata: {
                        idField:'ID',
                        maxRecordCount:sd.max_return_count,
                        limitExceeded: true
                    }
                }
                // console.log(JSON.stringify(data));
                return callback(null,data)
                
            },(error)=>{
                let jsonError = new Error ("Unable to execute SQL Query: " + error);
                jsonError.code = 400;
                return callback(jsonError);
            });
        }
    }
}

module.exports = Model