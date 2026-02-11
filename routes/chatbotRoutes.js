const express = require("express");
const router = express.Router();
const chatbotController = require("../controllers/chatbotController");

// Route to check if the server is running
router.get("/", chatbotController.getChatbotStatus);

// Route to handle webhook
router.post("/webhook", chatbotController.handleWebhook);

module.exports = router;
