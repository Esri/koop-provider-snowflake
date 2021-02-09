// clean shutdown on `cntrl + c`
process.on('SIGINT', () => process.exit(0))
process.on('SIGTERM', () => process.exit(0))

// Initialize Koop
const Koop = require('koop')
const koop = new Koop()


koop.register(require('./index'))
koop.server.listen(8080, () => console.log(`Koop listening on port 8080!`))


const message = `
Koop Snowflake Select Provider listening on 8080
Press control + c to exit
`
console.log(message)