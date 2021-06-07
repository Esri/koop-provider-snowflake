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

// clean shutdown on `cntrl + c`
process.on('SIGINT', () => process.exit(0))
process.on('SIGTERM', () => process.exit(0))

// Initialize Koop
const Koop = require('koop')
const koop = new Koop()


koop.register(require('./index'), {after: (request, geojson, callback) => {
    console.log(JSON.stringify(geojson));
    callback(null, geojson)}})
koop.server.listen(8080, () => console.log(`Koop listening on port 8080!`))


const message = `
Koop Snowflake Select Provider listening on 8080
Press control + c to exit
`
console.log(message)