const chatbotQueue = require("../utils/bull");
const axios = require("axios");
const db = require("../config/db");
const logger = require("../utils/logger");

// Process jobs from the queue
chatbotQueue.process("processWebhook", async (job, done) => {
    const { sender, receiver, message, fullPayload } = job.data;
    logger.info(`🔄 Processing job ID: ${job.id} - Sender: ${sender}`);

    try {
        // ✅ Fetch chatbot configuration
        const [userRows] = await db.query(
            "SELECT username FROM ci_admin WHERE mobile_no = ?",
            [sender]
        );

        if (userRows.length === 0) {
            logger.info(`No username found for Sender: ${sender}`);
            return done(null, { status: "No user found" });
        }

        const username = userRows[0].username;

        const [watiRows] = await db.query(
            "SELECT url, api_key FROM wati WHERE username = ?",
            [username]
        );

        if (watiRows.length === 0) {
            logger.info(`No WATI configuration found for Username: ${username}`);
            return done(null, { status: "No WATI configuration found" });
        }

        const { url, api_key } = watiRows[0];
        const META_API_URL = `${url}/messages`;

        // Process message types (sendMessage, buttons, etc.)
        switch (fullPayload.type) {
            case "sendMessage":
                await sendTextMessage(receiver, message, META_API_URL, api_key);
                break;

            case "buttons":
                await sendButtons(receiver, fullPayload.buttons, META_API_URL, api_key);
                break;

            case "list":
                await sendList(receiver, fullPayload.list, META_API_URL, api_key);
                break;

            default:
                logger.info("Unhandled message type");
        }

        // ✅ Log completion
        logger.info(`✅ Job completed successfully for Job ID: ${job.id}`);
        done(null, { status: "Job completed" });
    } catch (error) {
        logger.error("❌ Failed to process job:", error);
        done(error);
    }
});

// Send a simple text message
async function sendTextMessage(receiver, text, apiUrl, apiKey) {
    const payload = {
        messaging_product: "whatsapp",
        to: receiver,
        type: "text",
        text: { body: text },
    };
    const headers = {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
    };

    const response = await axios.post(apiUrl, payload, { headers });
    logger.info(`✅ Text message sent: ${response.data}`);
}

// Send buttons
async function sendButtons(receiver, buttons, apiUrl, apiKey) {
    const payload = {
        messaging_product: "whatsapp",
        to: receiver,
        type: "interactive",
        interactive: {
            type: "button",
            action: { buttons },
        },
    };

    const headers = {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
    };

    const response = await axios.post(apiUrl, payload, { headers });
    logger.info(`✅ Button message sent: ${response.data}`);
}

// Send list messages
async function sendList(receiver, list, apiUrl, apiKey) {
    const payload = {
        messaging_product: "whatsapp",
        to: receiver,
        type: "interactive",
        interactive: {
            type: "list",
            action: { sections: list.sections },
        },
    };

    const headers = {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
    };

    const response = await axios.post(apiUrl, payload, { headers });
    logger.info(`✅ List message sent: ${response.data}`);
}
