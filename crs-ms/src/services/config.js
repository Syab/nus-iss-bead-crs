require('dotenv').config()

let rest_proxy
if (process.env.NODE_ENV === 'production'){
    rest_proxy = process.env.REST_PROXY_BASE_URI
} else {
    rest_proxy = "http://localhost:8082"
}

module.exports = {
    PORT : process.env.PORT || 8000,
    REST_PROXY_BASE_URI : rest_proxy,
    HDB_API : "https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c&limit=4352",
    HDB_INFO_TOPIC_NAME : "HDB_CPKINFO",
    HDB_INFO_SCHEMA_ID : 1,
    HDB_AVAIL_TOPIC_NAME : "HDB_CPKAVAILABILITY",
    HDB_AVAIL_SCHEMA_ID: 2,
    HDB_AVAILABILITY_API: "https://api.data.gov.sg/v1/transport/carpark-availability"
};