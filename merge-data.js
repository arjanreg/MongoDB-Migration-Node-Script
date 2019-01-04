const mongodb = require('mongodb')
const async = require('async')
const customerData = require('./data/m3-customer-data.json')
const addressData = require('./data/m3-customer-address-data.json')
const numObjects = parseInt(process.argv[2]) || customerData.length
const collectionName = 'customers'

var arr = []

mongodb.MongoClient.connect('mongodb://localhost:27017', (error, client) => {
    if (error) return process.exit(1)
    console.log('Successfully connected to the server!')
    const db = client.db('assignment-module3-db')

    
    // loop through the customer-data file
    customerData.forEach((customer, index) => {
        customer = Object.assign(customer, addressData[index])
        if(index % numObjects == 0) {
            var missingObjects = index + numObjects
            if(missingObjects > customerData.length) {
                missingObjects = customerData.length
            }
        
        arr.push((callback) => {
            console.log("Merging the records...)
            db.collection(collectionName).insertMany(customerData.slice(index, missingObjects), (error, results) => {
                callback(error)
            })
        })
        }
    })
    
    async.parallel(arr, (error, results) => {
        if (error) console.error(error)
        console.log("Records have been copied.")
        client.close()
    })
    
})

