const axios = require('axios');
const {
    TOPIC_NAME,
    SCHEMA_ID
} = require("../services/config");

const hdbinfo = () => {
    console.log('hello from hdb service')
}

module.exports = {
    getHDBCarparkInfo
}