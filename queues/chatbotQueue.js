const Queue = require("bull");
const redisClient = require("../utils/redis");

// Create the chatbotQueue using Bull
const chatbotQueue = new Queue("chatbotQueue", {
  createClient: function (type) {
    switch (type) {
      case "client":
        return redisClient;
      case "subscriber":
        return redisClient.duplicate(); // For pub/sub
      default:
        return redisClient;
    }
  },
});

chatbotQueue.on("error", (err) => {
  console.error("❌ Error in chatbotQueue:", err);
});

chatbotQueue.on("completed", (job, result) => {
  console.log(`✅ Job completed: ID ${job.id}, Result:`, result);
});

chatbotQueue.on("failed", (job, err) => {
  console.error(`❌ Job failed: ID ${job.id}, Error:`, err);
});

module.exports = chatbotQueue;
