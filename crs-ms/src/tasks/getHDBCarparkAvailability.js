const axios = require('axios');
const kafkaService = require('../services/kafkaService');
const logger = require('../services/logger');
const DateUtil = require('../services/dateutil');
const {
    HDB_AVAIL_TOPIC_NAME,
    HDB_AVAIL_SCHEMA_ID,
    HDB_AVAILABILITY_API
} = require("../services/config");

const cvdate = new DateUtil();

const formatToSchema = (schemaId, data) => ({
    value_schema_id : schemaId,
    records: data.map((row) => ({
        value: row
    }))
})

const getHDBCarparkAvailability = () => {
    logger.log('Getting Carpark availability Info')
    axios
        .get(HDB_AVAILABILITY_API)
        .then((res)=> {
            if (res && res.data)
                logger.log("Call success")
                // console.log(res.data.items[0].timestamp)
                const formattedData = res.data.items[0].carpark_data.map(
                    (item) => ({
                        api_timestamp: cvdate.addXSeconds(res.data.items[0].timestamp.trim()).toString(),
                        total_lots: item.carpark_info[0].total_lots.trim(),
                        lot_type: item.carpark_info[0].lot_type.trim(),
                        lots_available: item.carpark_info[0].lots_available.trim(),
                        carpark_number: item.carpark_number.trim(),
                        update_datetime: cvdate.convertToSGT(item.update_datetime.trim()).toString()
                    })
                )
                // console.log(formattedData)
                const formatToRestProxy = formatToSchema(HDB_AVAIL_SCHEMA_ID, formattedData)

                console.log(formatToRestProxy)
                // console.log(formatToRestProxy['records'][0]['value']['api_timestamp'])
                logger.log("Pushing to Kafka")

                kafkaService
                    .pushToTopic(HDB_AVAIL_TOPIC_NAME, formatToRestProxy)
                    .then((res) => {
                        if (res && res.status === 200 && res.data.offsets[0].error === null)
                            logger.log(`Successfully pushed to topic ${HDB_AVAIL_TOPIC_NAME}`)
                        else if (res.data.offsets[0].error_code === 400)
                            console.log(res.data.offsets[0].message)
                    })
                    .catch((err) => {
                        logger.error("error pushing to topic 2", err)
                    })
        })
        .catch((err) => console.log("some error :" , err))
}

module.exports = {
    getHDBCarparkAvailability
}