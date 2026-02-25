// src/app.js
require("dotenv").config();
const express = require("express");
const morgan = require("morgan");
const { router: bullDashboard } = require("bull-board");
const chatbotQueue = require("./utils/bull");
const redisClient = require("./utils/redis");
const db = require("./config/db");

const app = express();
const PORT = process.env.PORT || 7700;

// Background cleanup — runs every 5 minutes instead of on every request
setInterval(async () => {
  try {
    await db.query("DELETE FROM chatbot_session WHERE expiry_time < NOW()");
    await db.query("DELETE FROM chatbot_default_cooldown_flow WHERE expiry_time < NOW()");
  } catch (err) {
    console.error("❌ Background cleanup error:", err.message);
  }
}, 5 * 60 * 1000);

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
