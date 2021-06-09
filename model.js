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
const turf = require('@turf/turf');
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
                        console.log(error.message);
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
    return turf.toWgs84(geoJson);

}
function esriGeometryToGeoJson(geometry,type){
    //@todo - handle other types
    switch (type){
        case 'esriGeometryEnvelope':
            let xmax = geometry.xmax;
            let xmin = geometry.xmin;
            let ymax = geometry.ymax;
            let ymin = geometry.ymin;
            
            return turf.polygon([[[xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin], [xmax, ymin]]]);
    }

}
function requestToSpatialFilter(req) {
    if (typeof req.query.geometry !== 'undefined') {
        let reqGeometry = req.query.geometry;
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
    if (typeof req.query.where !== 'undefined' && req.query.where !== '') {
        return req.query.where;
    }
    else{
        return '';
    }
}

function requestToLayerInfo(req){
    let name = req.params.id
    let idx = req.params.layer
    if(idx !== null && name !== null){
        let sd = serviceDefs.get(name)
        let data = {
            type: 'FeatureCollection',
            features: [],
            metadata: {
                idField:'ID',
                name:sd[idx].serviceDesc,
                maxRecordCount:sd[idx].max_return_count,
                limitExceeded: false,
                geometryType:sd[idx].geometryType,
                renderer: {
                    type: "simple",
                    symbol: {
                        type: "esriSMS",
                        style: "esriSMSCircle",
                        color: [133, 145, 58, 255],
                        size: 4,
                        angle: 0,
                        xoffset: 0,
                        yoffset: 0,
                        outline: {
                            color: [0, 0, 0, 255],
                            width: 0.7
                        }
                    }
                },
                fields: sd[idx].fields
            },
            capabilities: {
                quantization: false
            }
        }

        return data;
    }

}
// determines whether or not to use the resultRecordCount in the query params or maxRecordCount in the config file
function getMaxRows(resultRecordCount, supportsPagination, maxRecordCount) {
    if (resultRecordCount) {
        if (supportsPagination) {
            return resultRecordCount;
        }
        else {
            console.log("Warning: resultRecordCount not valid unless supportsPagination is true. Using maxReturnCount specified in the config file");
            return maxRecordCount;
        }
    } 
    else {
        return maxRecordCount;
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

    if (req.path.endsWith('query')){
        let name = req.params.id;
        let idx = req.params.layer;

        let returnCountOnly = req.query.returnCountOnly;
        let offset = req.query.resultOffset
        let resultRecordCount = req.query.resultRecordCount;

        if(idx !== null && name !== null){

            let sd = serviceDefs.get(name)
            let wkt = requestToSpatialFilter(req);
            let where = requestToWhereClause(req);
            let supportsPagination = sd[idx].supportsPagination;
            let maxRows = getMaxRows(resultRecordCount, supportsPagination, sd[idx].maxReturnCount);
            
            let snowflake_stmt;
            if (returnCountOnly) {
                snowflake_stmt = snowflake.count({"tableName": sd[idx].tableName, 
                                                  "spatialFilter": wkt,
                                                  "geographyField": sd[idx].geographyField, 
                                                  "whereClause": where
                                                });
            } 
            else {
                snowflake_stmt = snowflake.query({"tableName": sd[idx].tableName, 
                                                  "select": sd[idx].fields.map((item) => { return item.name }).join(','),
                                                  "geographyField": sd[idx].geographyField, 
                                                  "spatialFilter":wkt, 
                                                  "whereClause": where, 
                                                  "maxRows": maxRows,
                                                  "supportsPagination": supportsPagination,
                                                  "primaryId": sd[idx].primaryId, 
                                                  "offset": offset
                                                });
            }

            snowflake_stmt.then((result)=>{
                let data = {
                    type: 'FeatureCollection',
                    features: result,
                    metadata: {
                        idField:'ID',
                        maxRecordCount:sd[idx].max_return_count,
                        limitExceeded: false
                    }
                }

                if (returnCountOnly) {
                    data.count = result
                }
                return callback(null,data)
                
            },(error)=>{
                let jsonError = new Error ("Unable to execute SQL Query: " + error);
                jsonError.code = 400;
                return callback(jsonError);
            });
        }
    }
    else if (req.path.match(/\d+$/)){
        callback(null,requestToLayerInfo(req));
    }
    else {
        let err = new Error("Not implemented");
        err.code = 404;
        callback(err);
    }
}

module.exports = Model