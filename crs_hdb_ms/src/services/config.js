import 'dotenv/config';

// const PORT = process.env.PORT || 8000

module.exports = {
    PORT : process.env.PORT || 8000,
    REST_PROXY_BASE_URI : "http://localhost:8082",
    HDB_API : "https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c&limit=5",
    TOPIC_NAME : "HDB_CPK_INFO",
    SCHEMA_ID : 9
};