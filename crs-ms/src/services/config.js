import 'dotenv/config';

module.exports = {
    PORT : process.env.PORT || 8000,
    REST_PROXY_BASE_URI : "http://localhost:8082",
    HDB_API : "https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c&limit=4352",
    HDB_INFO_TOPIC_NAME : "HDB_CPKINFO",
    HDB_INFO_SCHEMA_ID : 5,
    HDB_AVAIL_TOPIC_NAME : "HDB_CPKAVAILABILITY",
    HDB_AVAIL_SCHEMA_ID: 4,
    HDB_AVAILABILITY_API: "https://api.data.gov.sg/v1/transport/carpark-availability"
};