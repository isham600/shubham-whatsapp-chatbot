// utils/bull.js
const Queue = require("bull");
const redisConfig = {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: process.env.REDIS_PORT || 6379,
    password: process.env.REDIS_PASSWORD || "",
};

// Create the chatbot queue
const chatbotQueue = new Queue("chatbotQueue", {
    redis: redisConfig,
});

// Log queue events for debugging
chatbotQueue.on("error", (error) => {
    console.error("❌ Queue Error:", error);
});
chatbotQueue.on("waiting", (jobId) => {
    console.log(`🔄 Job waiting with ID: ${jobId}`);
});
chatbotQueue.on("completed", (job) => {
    console.log(`✅ Job completed with ID: ${job.id}`);
});

module.exports = chatbotQueue;
