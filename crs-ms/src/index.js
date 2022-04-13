const serverless = require('serverless-http');
const cors = require('cors');
const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const { PORT, REST_PROXY_BASE_URI } = require('./services/config');
const tasks = require("./tasks");
const SVY21 = require('./services/svy21');
const logger = require('./services/logger');

const app = express();
const cv = new SVY21();

app.use(cors());
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE' );
    res.header('Access-Control-Allow-Origin', '*');
    res.header('AAccess-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, filename');
    next()
    ;});
app.use(bodyParser.text());
app.use(bodyParser.json());
app.use(express.static('.'))
app.use(bodyParser.urlencoded({extended: true,}));

app.get('/', (req, res) => {
    logger.log('testing logger')
    res.send('ROOT OK')
})

app.get('/health', async (req, res) => {
    const response = await axios(`${REST_PROXY_BASE_URI}/topics`,{
        method: "GET"
    })
    if (response && response.data && response.status===200){
        logger.log(`Call to REST Endpoint OK : ${response.status}`)
    } else {
        console.log(response)
        logger.error(`Something Bad Happened: ${response.status}`, response)
    }
    res.json('Call to REST Endpoint')

})

app.get('/hdbcpkinfo', async (req, res) => {
    logger.log(`Retrieving CPK Info`)
    tasks.getHDBCarparkInfo();
    res.status(200).send("Getting Carpark Info")
});

app.get('/hdbcpkavailability', async (req, res) => {
    logger.log(`Retrieving CPK Availability`)
    await tasks.getHDBCarparkAvailability();
    res.status(200).send("Getting availability Info")
})

app.get('/convert', async (req, res) => {
    const y_coord = req.body.y_coord;
    const x_coord = req.body.x_coord;
    // console.log(req.body)
    const resultLatLon = cv.computeLatLon(y_coord, x_coord);
    console.log(resultLatLon);
    res.json(JSON.stringify(resultLatLon['lat'])+','+JSON.stringify(resultLatLon['lat']))
})

app.get('/test', async (req, res) => {
    const response = await axios('https://api.data.gov.sg/v1/transport/carpark-availability',{
        method: "GET"
    })
    logger.log(response.data.items[0].timestamp)
    res.json('test')
})


app.listen(PORT, () =>{

    console.log(`Server Started on port : ${PORT}`)
    }
);

module.exports.handler = serverless(app);