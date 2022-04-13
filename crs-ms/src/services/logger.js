const pino = require('pino');
// const chalk = require('chalk');

const logger = pino({
    transport: {
        target: 'pino-pretty',
        options: {
            colorize: true,
            timestampKey: 'time',
            translateTime: "SYS:yyyy-mm-dd HH:MM:sso",
            ignore: 'pid,hostname',
            errorLikeObjectKeys: ['err', 'error'],
            messageKey: "message",
        }
    }
});

module.exports = {
    log: (message,data) => {
        logger.info({message, data})
    },
    error: (message, err) => {
        logger.error({message, err})
    }
}