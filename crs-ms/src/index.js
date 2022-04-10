import cors from 'cors';
import express from 'express';
// import bodyParser from 'body-parser';
import { PORT } from './services/config'

const tasks = require("./tasks")

const app = express();

app.use(cors());
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE' );
    res.header('Access-Control-Allow-Origin', '*');
    res.header('AAccess-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, filename');
    next()
    ;});

app.get('/health', (req, res) => {
    res.send('Hey OK')
})

app.get('/hdbcpkinfo', async (req, res) => {
    tasks.getHDBCarparkInfo();
    res.status(200).send("Getting Carpark Info")
});

app.get('/hdbcpkavailability', async (req, res) => {
    tasks.getHDBCarparkAvailability();
    res.status(200).send("Getting availability Info")
})

app.listen(PORT, () =>
    console.log(`Server Started on port : ${PORT}`)
);

