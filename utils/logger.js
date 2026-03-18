const { createLogger, format, transports } = require("winston");
const { combine, timestamp, printf, splat } = format;

// Custom log format — includes any extra splat arguments (e.g. error.message, error.stack)
const logFormat = printf(({ level, message, timestamp, [Symbol.for('splat')]: splatArgs }) => {
    const extra = splatArgs
        ? splatArgs.map(a => (a instanceof Error ? a.stack : (typeof a === 'object' ? JSON.stringify(a) : a))).join(' ')
        : '';
    return `${timestamp} [${level.toUpperCase()}]: ${message}${extra ? ' ' + extra : ''}`;
});

// Create logger instance
const logger = createLogger({
    level: "info",
    format: combine(timestamp(), splat(), logFormat),
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
