const { createLogger, format, transports } = require("winston");
const { combine, timestamp, printf } = format;

// Custom log format
const logFormat = printf(({ level, message, timestamp }) => {
    return `${timestamp} [${level.toUpperCase()}]: ${message}`;
});

// Create logger instance
const logger = createLogger({
    level: "info",
    format: combine(timestamp(), logFormat),
    transports: [
        new transports.File({ filename: "logs/chatbot.log" }),
        new transports.Console(),
    ],
});

// Stream for morgan
logger.stream = {
    write: (message) => {
        logger.info(message.trim());
    },
};

module.exports = logger;
