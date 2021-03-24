# koop-provider-snowflake

A configurable and reusable [Snowflake](https://www.snowflake.com/) provider for [Koop](http://koopjs.github.io/)

This provider creates a connection to Snowflake using the Snowflake supported [Node.js driver](https://docs.snowflake.com/en/user-guide/nodejs-driver.html) and executes SQL queries against a configureable list of tables. Snowflake tables must contain a [GEOGRAPHY](https://docs.snowflake.com/en/sql-reference/data-types-geospatial.html) column.

## Features

- configurable
- supports multiple Snowflake table sources
- supports `query` operation with `where` and `geometry` parameters
    * only `esriGeometryEnvelope` is supported at this time


## Usage

Once started, a Koop server including this provider supports routes like

```
/snowflake-provider/:host/:id/FeatureServer/:layer/query
```

where `host` can be any preferred value, `id` is the key used to look up a service definition in the configurable map of `serviceDefinitions` and `layer` is the index of a item in the service definition. See configuration instructions below.

For example, this route allows you to query the first configured service definition.

```
/snowflake-provider/rest/locations/FeatureServer/0/query
```

## Configuration

The configuration includes required properties to create a Snowflake connection. The `serviceDefinitions` map enables unique service endpoints to expose the `query` operation on Snowflake tables.

`serviceDefinitions` 
 - `serviceIndex` is the unique ID for this query endpoint, which targets one table.
 - `tableName` is the qualified Snowflake table name.
 - `geographyField` is the name of the required `GEOGRAPHY` column.
 - `selectFields` is the `SELECT` component of the SQL statement. For security purposes the client is not allowed to control the returned fields. At this time the client can't specify a subset of these fields.
 - `maxReturnCount` becomes the `TOP n` component of the SQL statement.


A configuration looks like this:

```javascript
{
    "koopProviderSnowflake":{
        "snowflakeAccount":"<Your Account>",
        "snowflakeWarehouse":"<Your Data Warehouse>",
        "snowflakeUser":"<Your Username>",
        "snowflakePassword":"<Your Password>",
        "serviceDefinitions":{
            "locations": [
                {
                "serviceIndex":0,
                "serviceDesc":"Sample location data",
                "tableName":"DEMO.PUBLIC.LOCATIONS",
                "geographyField":"SHAPE",
                "selectFields":"ID,LOB,CONSTTYPE,YEARBUILT,LATITUDE,LONGITUDE",
                "maxReturnCount":5000,
                "geometryType":"esriGeometryPoint"
                }
            ]
        }
    }
}

```


## Development

### Run dev server

with Node.js

``` bash
node server.js
```

A dev server will be running at `http://localhost:8080`. By default, it will use the default configuration `config/default.json`, which should be updated beforehand.

## License
Copyright 2021 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [License.txt](https://github.com/EsriPS/koop-provider-snowflake/blob/main/License.txt) file.