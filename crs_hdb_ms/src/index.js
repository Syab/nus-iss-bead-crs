import cors from 'cors';
import express from 'express';
// import bodyParser from 'body-parser';
import { PORT } from './services/config'
import axios from "axios";

const app = express();
const hdbInfo = require('../src/tasks')
const datagov_url = "https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c&limit=5"

app.use(cors());
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE' );
    res.header('Access-Control-Allow-Origin', '*');
    res.header('AAccess-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, filename');
    next()
    ;});

app.get('/h', (req, res) => {
    res.send('Hey OK')
})

app.get('/hdbinfo', async (req, res, next) => {
    console.log("getting HDB info");
    let cpk_info = []
    try {
        const data = await axios.get(datagov_url)
        const total_cpk = data.data.result.total
        if ( total_cpk !== 0 ){
            res.json(data.data.result.records);
            cpk_info = data.data.result.records
            cpk_info.forEach( idx => console.log(idx))
        } else {
            res.json("Unable to get any cpk!");
        }
    }
    catch (err){
        next(err)
    }
});

app.listen(PORT, () =>
    console.log(`Server Started on port : ${PORT}`)
);

