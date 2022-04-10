const axios = require('axios');
const kafkaService = require('../services/kafkaService')
const {
    HDB_INFO_TOPIC_NAME,
    HDB_INFO_SCHEMA_ID,
    HDB_API
} = require("../services/config");

const formatToSchema = (schemaId, data) => ({
    value_schema_id : schemaId,
    records: data.map((row) => ({
        value: row
    }))
})

const getHDBCarparkInfo = () => {
    console.log("Getting HDB Carpark info");
    axios
        .get(HDB_API)
        .then((res) => {
            if (res && res.data && res.data.success)
                console.log("Call success")
                const formattedData = res.data.result.records.map(
                    (item) => ({
                        short_term_parking: item.short_term_parking.trim(),
                        car_park_type: item.car_park_type.trim(),
                        y_coord: item.y_coord.trim(),
                        x_coord: item.x_coord.trim(),
                        free_parking: item.free_parking.trim(),
                        gantry_height: item.gantry_height.trim(),
                        car_park_basement: item.car_park_basement.trim(),
                        night_parking: item.night_parking.trim(),
                        address: item.address.trim(),
                        car_park_decks: item.car_park_decks.trim(),
                        cpkid: item._id,
                        car_park_no: item.car_park_no.trim(),
                        type_of_parking_system: item.type_of_parking_system.trim()
                    })
                )
                const formatToRestProxy = formatToSchema(HDB_INFO_SCHEMA_ID, formattedData)
                console.log(formatToRestProxy)
                kafkaService
                    .pushToTopic(HDB_INFO_TOPIC_NAME, formatToRestProxy)
                    .then((res) => {
                        if (res && res.status === 200 && res.data.offsets[0].error === null)
                            console.log("Succesfully pushed to topic", HDB_INFO_TOPIC_NAME)
                        else if (res.data.offsets[0].error_code === 400)
                            console.log(res.data.offsets[0].message)
                    })
                    .catch((err) => {
                        console.log("error pushing to topic 2: ", err)
                    })
            })
        .catch((err) => console.log("some error :" , err))
}



module.exports = {
    getHDBCarparkInfo
}