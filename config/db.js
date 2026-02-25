require('dotenv').config(); // Load .env variables

const mysql = require('mysql2');

// Create a connection pool using env vars
const pool = mysql.createPool({
    host: process.env.DB_HOST || '127.0.0.1', // fallback for local dev
    port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : 3306,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    waitForConnections: true,
    connectionLimit: 50,
    queueLimit: 0,
});

// Promisify the pool for async/await
const db = pool.promise();

module.exports = db;
