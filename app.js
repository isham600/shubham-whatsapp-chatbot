// src/app.js
require("dotenv").config();
const express = require("express");
const morgan = require("morgan");
const { router: bullDashboard } = require("bull-board");
const chatbotQueue = require("./utils/bull");
const redisClient = require("./utils/redis");

const app = express();
const PORT = process.env.PORT || 7700;

// Middleware
app.use(express.json());
app.use(morgan("combined"));

// Add Bull Dashboard for monitoring jobs
app.use("/chatbotflow/admin/queues", bullDashboard);

// Routes
const chatbotRoutes = require("./routes/chatbotRoutes");
app.use("/chatbotflow", chatbotRoutes);

// Start the server
app.listen(PORT, () => {
    console.log(`Chatbot running on port ${PORT}`);
});
