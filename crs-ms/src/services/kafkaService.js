const axios = require('axios');
const { REST_PROXY_BASE_URI } = require('./config');

module.exports = {
    pushToTopic : (topicName, requestBody) =>
        axios({
            url: `${REST_PROXY_BASE_URI}/topics/${topicName}`,
            method: "POST",
            headers: {
                "Content-Type": "application/vnd.kafka.avro.v2+json",
                Accept: "application/vnd.kafka.v2+json,application/vnd.kafka+json, application/json"
            },
            data: requestBody
        })
};
