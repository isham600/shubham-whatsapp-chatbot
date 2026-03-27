const db = require("../config/db"); // Database connection
const logger = require("../utils/logger"); // Logger utility
const stringSimilarity = require("string-similarity");
const axios = require("axios"); // HTTP requests
const config = require("./config"); // Configurations (e.g., session expiry, base URLs)
const chatbotService = require('./chatbotService');
const redisClient = require("../utils/redis"); // Redis for caching

// Cache TTL: 5 minutes
const CACHE_TTL = 300;

async function getCachedUsername(sender) {
  const key = `ci_admin:${sender}`;
  const cached = await redisClient.get(key);
  if (cached) return cached;
  const [rows] = await db.query("SELECT username FROM ci_admin WHERE mobile_no = ?", [sender]);
  if (rows.length === 0) return null;
  await redisClient.setex(key, CACHE_TTL, rows[0].username);
  return rows[0].username;
}

async function getCachedWati(username) {
  const key = `wati:${username}`;
  const cached = await redisClient.get(key);
  if (cached) return JSON.parse(cached);
  const [rows] = await db.query("SELECT url, api_key FROM wati WHERE username = ?", [username]);
  if (rows.length === 0) return null;
  await redisClient.setex(key, CACHE_TTL, JSON.stringify(rows[0]));
  return rows[0];
}

/** ========================================================================
 * ✅ COMPREHENSIVE LOGGING SYSTEM
 * ======================================================================== */

/**
 * ✅ Comprehensive logging function for chatbot PM2 logs
 * @param {Object} logData - Logging data object
 */
function logToPM2({
  username,
  flowId = null,
  nodeId = null,
  senderId,
  receiverId,
  senderName = null,
  logType,
  logLevel = 'info',
  message,
  requestPayload = null,
  responsePayload = null,
  userMessage = null,
  botResponse = null,
  processingTime = null,
  apiEndpoint = null,
  errorDetails = null,
  sessionData = null,
  variablesData = null,
  metadata = null,
  ipAddress = null,
  userAgent = null
}) {
  // Fire-and-forget: never blocks the caller
  const query = `
    INSERT INTO chatbot_pm2_logs (
      username, flow_id, node_id, sender_id, receiver_id, sender_name,
      log_type, log_level, message, request_payload, response_payload,
      user_message, bot_response, processing_time, api_endpoint,
      error_details, session_data, variables_data, metadata,
      ip_address, user_agent, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
  `;

  const values = [
    username,
    flowId,
    nodeId,
    senderId,
    receiverId,
    senderName,
    logType,
    logLevel,
    message,
    requestPayload ? JSON.stringify(requestPayload) : null,
    responsePayload ? JSON.stringify(responsePayload) : null,
    userMessage,
    botResponse,
    processingTime,
    apiEndpoint,
    errorDetails,
    sessionData ? JSON.stringify(sessionData) : null,
    variablesData ? JSON.stringify(variablesData) : null,
    metadata ? JSON.stringify(metadata) : null,
    ipAddress,
    userAgent
  ];

  db.query(query, values).catch(error => {
    logger.error("❌ Failed to save PM2 log:", error.message);
  });
}

/**
 * ✅ Save bot-sent message to chat_messages table (fire-and-forget)
 */
function saveChatMessage({ username, senderId, receiverId, senderName, type, text, media, whtsRefId, templateId, attributes, templateMedia }) {
  const attrValue = attributes
    ? (Array.isArray(attributes) ? '[]' : JSON.stringify(attributes))
    : '[]';

  const query = `
    INSERT INTO chat_messages
    (username, sender_id, receiver_id, status, eventDescription, replySourceMessage,
     text, media, type, eventtype, whts_ref_id, template_id, attributes, template_media,
     created_at, updated_at)
    VALUES (?, ?, ?, 1, 'Bot Replied', ?, ?, ?, ?, 'broadcastMessage', ?, ?, ?, ?, NOW(), NOW())
  `;

  db.query(query, [
    username, senderId, receiverId, senderName,
    text || null, media || null, type,
    whtsRefId || null, templateId || null,
    attrValue, templateMedia || null
  ]).catch(err => logger.error("❌ Failed to save chat_message:", err.message));
}

/** ========================================================================
 * ✅ MAIN WEBHOOK HANDLER
 * ======================================================================== */

exports.handleWebhook = async (req, res) => {
  const { data } = req.body;

  // Validate before responding
  if (!data) {
    logger.error("🚫 No data received in the request.");
    return res.status(400).json({ error: "Invalid request. 'data' field is missing." });
  }

  // ✅ Respond immediately — prevents WhatsApp/WATI from retrying and sending duplicates
  res.status(200).json({ status: "received" });

  // Process asynchronously in the background
  const ipAddress = req.ip || req.connection?.remoteAddress;
  const userAgent = req.get('User-Agent');
  processWebhookInBackground(data, ipAddress, userAgent)
    .catch(err => logger.error("❌ Background webhook error:", err.message));
};

async function processWebhookInBackground(data, ipAddress, userAgent) {
  const startTime = Date.now();
  let username = null;

  const sender = data.sender_id || "Unknown Sender";
  const receiver = data.waId || "Unknown Receiver";
  const message = data.text || "No Message";
  const sendername = data.senderName || "Unknown User";

  logger.info(`➡️ Incoming Webhook - Sender ID: ${sender}, Receiver ID: ${receiver}`);
  logger.info(`🔹📩 Sender: ${sender}, 📩 Receiver: ${receiver}, 📩 Message: "${message}"`);

  try {
    logToPM2({
      username: 'system',
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'webhook_received',
      logLevel: 'info',
      message: `Webhook received from ${sender}`,
      userMessage: message,
      metadata: {
        originalData: data,
        processingStarted: new Date().toISOString()
      },
      ipAddress,
      userAgent
    });

    // Session cleanup runs via background interval in app.js

    /** ========================================================================
     * ✅ STEP 1: GET USERNAME FROM ci_admin (Redis cached)
     * ======================================================================== */
    username = await getCachedUsername(sender);
    if (!username) {
      logToPM2({
        username: 'unknown',
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'flow_not_found',
        logLevel: 'warning',
        message: `No username found for sender: ${sender}`,
        userMessage: message,
        processingTime: (Date.now() - startTime) / 1000
      });
      return;
    }

    logger.info(`✅ Username found: ${username}`);

    /** ========================================================================
     * ✅ STEP 2: GET WATI API CONFIGURATION (Redis cached)
     * ======================================================================== */
    const watiConfig = await getCachedWati(username);
    if (!watiConfig) {
      logToPM2({
        username,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'system_error',
        logLevel: 'error',
        message: `No WATI configuration found for username: ${username}`,
        userMessage: message,
        processingTime: (Date.now() - startTime) / 1000
      });
      return;
    }

    const { url, api_key } = watiConfig;
    const META_API_URL = `${url}/messages`;
    const ACCESS_TOKEN = api_key;
    logger.info("✅ WATI API configuration loaded successfully.");

    /** ========================================================================
     * ✅ STEP 3: SEARCH THROUGH ALL ACTIVE SESSIONS
     * ======================================================================== */
    logger.info("🔍 Searching for all active sessions for sender and receiver...");
    const [allSessions] = await db.query(
      "SELECT * FROM chatbot_session WHERE sender_id = ? AND receiver_id = ? ORDER BY updated_at DESC",
      [sender, receiver]
    );

    let processedSession = false;

    for (const session of allSessions) {
      const sessionDetails = {
        flowId: session.flow_id,
        lastNodeId: session.source,
        sourceHandles: session.sourceHandle ? JSON.parse(session.sourceHandle || "[]") : [],
      };

      // ✅ CASE 1: Check sourceHandle
      if (sessionDetails.sourceHandles.length > 0) {
        const matchedHandle = sessionDetails.sourceHandles.find(
          (handleObj) => handleObj.sourceHandle === `handle-${message}`
        );
        if (matchedHandle) {
          await logToPM2({
            username,
            flowId: session.flow_id,
            nodeId: session.source,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'trigger_matched',
            logLevel: 'info',
            message: `Source handle matched: ${matchedHandle.sourceHandle}`,
            userMessage: message,
            sessionData: session,
            metadata: { matchedHandle }
          });

          sessionDetails.lastNodeId = matchedHandle.target;
          await db.query(
            "UPDATE chatbot_session SET source = ?, updated_at = NOW() WHERE id = ?",
            [matchedHandle.target, session.id]
          );

          await processChatbotFlow(
            sessionDetails,
            message,
            sender,
            receiver,
            META_API_URL,
            ACCESS_TOKEN,
            sendername,
            username
          );
          processedSession = true;
          break;
        }
      }

      // ✅ CASE 2: Check trigger_key
      if (session.trigger_key === message) {
        await logToPM2({
          username,
          flowId: session.flow_id,
          nodeId: session.source,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'trigger_matched',
          logLevel: 'info',
          message: `Trigger key matched: ${message}`,
          userMessage: message,
          sessionData: session
        });

        await db.query("DELETE FROM chatbot_session WHERE id = ?", [session.id]);
        logger.info(`✅ Session ${session.id} deleted. Proceeding to find a new flow...`);

        const matchedFlow = await findMatchingFlow(message, sender, receiver, username);
        if (matchedFlow) {
          await handleSession(sender, receiver, null, matchedFlow.id, "trigger", message);
          await processChatbotFlow(
            { flowId: matchedFlow.id, lastNodeId: null, sourceHandles: [] },
            message,
            sender,
            receiver,
            META_API_URL,
            ACCESS_TOKEN,
            sendername,
            username
          );
          processedSession = true;
          break;
        }
      }

      // ✅ CASE 3: AI node response handling
      if (session.message_type === "ai") {
        await logToPM2({
          username,
          flowId: session.flow_id,
          nodeId: session.source,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'ai_continued_conversation',
          logLevel: 'info',
          message: 'User continuing conversation in AI node',
          userMessage: message,
          sessionData: session
        });

        // Process the message through the AI node
        const [stepsRows] = await db.query("SELECT nodes FROM chatbot_steps WHERE flow_id = ?", [session.flow_id]);
        if (stepsRows.length > 0) {
          const nodes = JSON.parse(stepsRows[0].nodes || "[]");
          const aiNode = nodes.find(node => node.id === session.source);

          if (aiNode && aiNode.type === "ai") {
            await processAINode(aiNode, receiver, META_API_URL, ACCESS_TOKEN, session.flow_id, sender, [], sendername, username);
            return;
          }
        }

        // Fallback if AI node not found
        await sendTextMessage(receiver, "I'm sorry, there was an issue processing your message. Please try again.", META_API_URL, ACCESS_TOKEN, sender, session.flow_id, sendername);
        return;
      }

      // ✅ CASE 4: Question response handling
      if (session.message_type === "question") {
        await logToPM2({
          username,
          flowId: session.flow_id,
          nodeId: session.source,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'user_response',
          logLevel: 'info',
          message: `User responding to question: ${session.variable_name}`,
          userMessage: message,
          sessionData: session,
          metadata: { questionType: session.question_type }
        });

        // Store user response
        if (session.variable_name) {
          await storeUserResponse(sender, receiver, session.flow_id, session.variable_name, message, username, {
            nodeId: session.source,
            nodeType: session.message_type,
            questionText: session.trigger_key,
            senderName: sendername
          });
        }

        // Determine next node
        let nextNode = null;

        // Debug logging for edges
        logger.info(`🔍 Debug - Looking for next node from source: ${session.source}`);
        logger.info(`🔍 Debug - Available sourceHandles: ${JSON.stringify(sessionDetails.sourceHandles, null, 2)}`);

        if (!Array.isArray(sessionDetails.sourceHandles) || sessionDetails.sourceHandles.length === 0) {
          logToPM2({
            username,
            flowId: session.flow_id,
            nodeId: session.source,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'flow_error',
            logLevel: 'error',
            message: 'sourceHandles is empty or not an array',
            userMessage: message,
            sessionData: session
          });
          return;
        }

        const matchedHandle = sessionDetails.sourceHandles.find(
          (handleObj) => handleObj.source === session.source
        );

        if (!matchedHandle) {
          // Try to find any handle that contains the source node ID
          const fallbackHandle = sessionDetails.sourceHandles.find(
            (handleObj) => handleObj.sourceHandle.includes(session.source)
          );

          if (fallbackHandle) {
            nextNode = fallbackHandle.target;
            logger.info(`✅ Found fallback handle for source ${session.source}: ${fallbackHandle.target}`);
          } else {
            logToPM2({
              username,
              flowId: session.flow_id,
              nodeId: session.source,
              senderId: sender,
              receiverId: receiver,
              senderName: sendername,
              logType: 'flow_error',
              logLevel: 'error',
              message: `No matching handle found for source: ${session.source}`,
              userMessage: message,
              sessionData: session
            });
            return;
          }
        } else {
          nextNode = matchedHandle.target;
        }

        const defaultHandle = sessionDetails.sourceHandles.find(
          (handleObj) =>
            handleObj.source === session.source &&
            (handleObj.sourceHandle.startsWith(`handle-default-${handleObj.source}`) ||
             handleObj.sourceHandle.startsWith(`question-default-handle-${handleObj.source}`) ||
             handleObj.sourceHandle.includes(`-${handleObj.source}`))
        );

        if (defaultHandle) {
          nextNode = defaultHandle.target;
        }

        if (nextNode) {
          await updateSessionSource(sender, receiver, nextNode);
          await processNextNode(
            nextNode,
            sender,
            receiver,
            session.flow_id,
            message,
            META_API_URL,
            ACCESS_TOKEN,
            sendername,
            username
          );
          return;
        }

        logToPM2({
          username,
          flowId: session.flow_id,
          nodeId: session.source,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'chatbot_incomplete',
          logLevel: 'warning',
          message: 'No valid next node found for question response',
          userMessage: message,
          sessionData: session
        });
        return;
      }
    }

    // ✅ FALLBACK: No session processed
    if (!processedSession) {
      logToPM2({
        username,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'trigger_not_matched',
        logLevel: 'info',
        message: 'No matching session found, searching for new flow',
        userMessage: message
      });

      const matchedFlow = await findMatchingFlow(message, sender, receiver, username);
      if (matchedFlow) {
        logToPM2({
          username,
          flowId: matchedFlow.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'flow_started',
          logLevel: 'info',
          message: `New flow started: ${matchedFlow.name}`,
          userMessage: message,
          metadata: { flowDetails: matchedFlow }
        });

        const sessionDetails = {
          flowId: matchedFlow.id,
          lastNodeId: null,
          sourceHandles: [],
        };
        await handleSession(sender, receiver, null, matchedFlow.id, "trigger", message);
        await processChatbotFlow(
          sessionDetails,
          message,
          sender,
          receiver,
          META_API_URL,
          ACCESS_TOKEN,
          sendername,
          username
        );
      } else {
        logToPM2({
          username,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'flow_not_found',
          logLevel: 'warning',
          message: 'No matching flow found for incoming message',
          userMessage: message,
          processingTime: (Date.now() - startTime) / 1000
        });
        return;
      }
    }

    logToPM2({
      username,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'webhook_processed',
      logLevel: 'info',
      message: 'Webhook processed successfully',
      userMessage: message,
      processingTime: (Date.now() - startTime) / 1000
    });

  } catch (error) {
    logToPM2({
      username: username || 'unknown',
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'system_error',
      logLevel: 'error',
      message: `Webhook processing failed: ${error.message}`,
      userMessage: message,
      errorDetails: error.stack,
      processingTime: (Date.now() - startTime) / 1000
    });

    logger.error("❌ Error in webhook processing:", error.message);
    logger.debug("Stack Trace:", error.stack);

    try {
      await insertLog(null, "Unknown", sender, receiver, "systemError", error.message);
    } catch (logError) {
      logger.error("❌ Failed to insert error log into chatbot_logs table:", logError.message);
    }
  }
}

/** ========================================================================
 * ✅ FIND MATCHING FLOW UTILITY
 * ======================================================================== */

async function findMatchingFlow(message, sender, receiver, username) {
  try {
    logger.info("🔍 Searching for a matching chatbot flow...");

    const [flowRows] = await db.query(
      "SELECT * FROM chatbot_flows WHERE username = ? AND status = 'active'",
      [username]
    );

    if (!flowRows.length) {
      await logToPM2({
        username,
        senderId: sender,
        receiverId: receiver,
        logType: 'flow_not_found',
        logLevel: 'info',
        message: 'No active flows found for username',
        userMessage: message
      });
      return null;
    }

    const [sessionRows] = await db.query(
      "SELECT * FROM chatbot_session WHERE sender_id = ? AND receiver_id = ?",
      [sender, receiver]
    );

    const hasActiveSession = sessionRows.length > 0;
    let defaultFlow = null;
    let bestFuzzyMatch = null;
    let bestFuzzyMatchRating = 0;

    for (const flow of flowRows) {
      try {
        const triggerKeys = JSON.parse(flow.trigger_key || "[]");
        if (!Array.isArray(triggerKeys)) {
          logger.error(`🚫 Invalid trigger_key format in Flow ID: ${flow.id}`);
          continue;
        }

        // Handle Exact Matching
        if (flow.matching_method === "Exact Matching") {
          if (triggerKeys.includes(message)) {
            await logToPM2({
              username,
              flowId: flow.id,
              senderId: sender,
              receiverId: receiver,
              logType: 'trigger_matched',
              logLevel: 'info',
              message: `Exact match found in flow: ${flow.name}`,
              userMessage: message,
              metadata: { matchingMethod: 'exact', flowName: flow.name }
            });
            return flow;
          }
        }

        // Handle Fuzzy Matching
        if (flow.matching_method === "Fuzzy Matching") {
          const bestMatch = stringSimilarity.findBestMatch(message, triggerKeys);
          if (bestMatch.bestMatch.rating * 100 >= (flow.fuzzy_logic_percentage || 0)) {
            if (bestMatch.bestMatch.rating > bestFuzzyMatchRating) {
              bestFuzzyMatch = flow;
              bestFuzzyMatchRating = bestMatch.bestMatch.rating;
            }
          }
        }

        // Handle Default Flow
        if (triggerKeys.includes("default")) {
          if (!defaultFlow) {
            defaultFlow = flow;
          }
        }
      } catch (flowError) {
        await logToPM2({
          username,
          flowId: flow.id,
          senderId: sender,
          receiverId: receiver,
          logType: 'flow_error',
          logLevel: 'error',
          message: `Error processing flow: ${flow.name}`,
          userMessage: message,
          errorDetails: flowError.message
        });
        continue;
      }
    }

    if (bestFuzzyMatch) {
      await logToPM2({
        username,
        flowId: bestFuzzyMatch.id,
        senderId: sender,
        receiverId: receiver,
        logType: 'trigger_matched',
        logLevel: 'info',
        message: `Fuzzy match found in flow: ${bestFuzzyMatch.name}`,
        userMessage: message,
        metadata: { matchingMethod: 'fuzzy', rating: bestFuzzyMatchRating, flowName: bestFuzzyMatch.name }
      });
      return bestFuzzyMatch;
    }

    if (defaultFlow && !hasActiveSession) {
      // Cooldown cleanup runs via background interval in app.js

      const [cooldownRows] = await db.query(
        `SELECT * FROM chatbot_default_cooldown_flow 
         WHERE sender_id = ? AND receiver_id = ? AND flow_id = ? AND expiry_time > NOW()`,
        [sender, receiver, defaultFlow.id]
      );

      if (cooldownRows.length > 0) {
        await logToPM2({
          username,
          flowId: defaultFlow.id,
          senderId: sender,
          receiverId: receiver,
          logType: 'trigger_not_matched',
          logLevel: 'info',
          message: `Default flow cooldown active until ${cooldownRows[0].expiry_time}`,
          userMessage: message
        });
        return null;
      }

      const expiry = new Date(Date.now() + 8 * 60 * 60 * 1000);
      await db.query(
        `INSERT INTO chatbot_default_cooldown_flow 
         (sender_id, receiver_id, flow_id, expiry_time) 
         VALUES (?, ?, ?, ?) 
         ON DUPLICATE KEY UPDATE expiry_time = ?`,
        [sender, receiver, defaultFlow.id, expiry, expiry]
      );

      await logToPM2({
        username,
        flowId: defaultFlow.id,
        senderId: sender,
        receiverId: receiver,
        logType: 'trigger_matched',
        logLevel: 'info',
        message: `Default flow triggered: ${defaultFlow.name}`,
        userMessage: message,
        metadata: { matchingMethod: 'default', flowName: defaultFlow.name, cooldownSet: expiry }
      });

      return defaultFlow;
    }

    return null;
  } catch (error) {
    await logToPM2({
      username,
      senderId: sender,
      receiverId: receiver,
      logType: 'system_error',
      logLevel: 'error',
      message: `Error in findMatchingFlow: ${error.message}`,
      userMessage: message,
      errorDetails: error.stack
    });
    throw error;
  }
}


/** ========================================================================
 * ✅ PROCESS CHATBOT FLOW
 * ======================================================================== */

async function processChatbotFlow(
  sessionDetails,
  message,
  sender,
  receiver,
  apiUrl,
  accessToken,
  sendername,
  username
) {
  const startTime = Date.now();

  try {
    await logToPM2({
      username,
      flowId: sessionDetails.flowId,
      nodeId: sessionDetails.lastNodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'flow_started',
      logLevel: 'info',
      message: 'Starting chatbot flow processing',
      userMessage: message,
      sessionData: sessionDetails
    });

    logger.info("🔄 Starting chatbot flow processing.");

    const [stepsRows] = await db.query(
      "SELECT nodes, edges FROM chatbot_steps WHERE flow_id = ?",
      [sessionDetails.flowId]
    );

    if (!stepsRows.length) {
      throw new Error(`❌ No steps found for Flow ID: ${sessionDetails.flowId}`);
    }

    let nodes = JSON.parse(stepsRows[0].nodes || "[]");
    let edges = JSON.parse(stepsRows[0].edges || "[]");

    nodes = nodes.map((node) => ({ ...node, id: node.id.toString() }));
    edges = edges.map((edge) => ({
      ...edge,
      source: edge.source?.toString(),
      target: edge.target?.toString()
    }));

    // Validate edges against nodes
    const nodeIds = nodes.map((node) => node.id);
    const invalidEdges = edges.filter((edge) => {
      const isSourceValid = nodeIds.includes(edge.source);
      const isTargetValid = nodeIds.includes(edge.target);
      return !isSourceValid || !isTargetValid;
    });

    if (invalidEdges.length) {
      throw new Error(`❌ Invalid edges detected for Flow ID: ${sessionDetails.flowId}`);
    }

    // Determine start node
    let currentNodeId;
    let reason;

    if (sessionDetails.lastNodeId) {
      currentNodeId = sessionDetails.lastNodeId.toString();
      reason = "Using last processed node from sessionDetails";
    } else {
      const startNode = nodes.find((node) => node.data?.isStartNode === "true");
      if (startNode) {
        currentNodeId = startNode.id;
        reason = "Using node with isStartNode: true";
      } else {
        currentNodeId = nodes[0]?.id;
        reason = "No start node found, defaulting to first node";
      }
    }

    if (!currentNodeId) {
      throw new Error("❌ No valid starting node found.");
    }

    // Main flow loop
    while (currentNodeId) {
      logger.info(`🔄 Processing Node ID: ${currentNodeId}`);

      const currentNode = nodes.find((node) => node.id === currentNodeId);
      if (!currentNode) {
        throw new Error(`❌ Node with ID: ${currentNodeId} not found.`);
      }

      const isPauseNode = ["buttons", "list", "question"].includes(currentNode.type);

      await handleSession(
        sender,
        receiver,
        currentNodeId,
        sessionDetails.flowId,
        isPauseNode ? currentNode.type : "normal",
        message,
        edges
      );

      await processNode(
        currentNode,
        receiver,
        apiUrl,
        accessToken,
        sessionDetails.flowId,
        sender,
        edges,
        sendername,
        username
      );

      if (isPauseNode) {
        await logToPM2({
          username,
          flowId: sessionDetails.flowId,
          nodeId: currentNodeId,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'flow_paused',
          logLevel: 'info',
          message: `Flow paused after ${currentNode.type} node`,
          userMessage: message,
          processingTime: (Date.now() - startTime) / 1000,
          metadata: { nodeType: currentNode.type }
        });
        return;
      }

      const nextEdge = edges.find((edge) => edge.source === currentNodeId);
      currentNodeId = nextEdge ? nextEdge.target : null;
    }

    await logToPM2({
      username,
      flowId: sessionDetails.flowId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'flow_completed',
      logLevel: 'info',
      message: 'Chatbot flow completed successfully',
      userMessage: message,
      processingTime: (Date.now() - startTime) / 1000
    });

    logger.info("✅ Chatbot flow processed successfully.");
  } catch (error) {
    await logToPM2({
      username,
      flowId: sessionDetails.flowId,
      nodeId: sessionDetails.lastNodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'flow_error',
      logLevel: 'error',
      message: `Error in processChatbotFlow: ${error.message}`,
      userMessage: message,
      errorDetails: error.stack,
      processingTime: (Date.now() - startTime) / 1000
    });
    
    logger.error("❌ Error in processChatbotFlow:", error.message);
    throw error;
  }
}

/** ========================================================================
 * ✅ PROCESS INDIVIDUAL NODES
 * ======================================================================== */

async function processNode(
  currentNode,
  receiver,
  apiUrl,
  accessToken,
  flowId,
  sender,
  edges,
  sendername,
  username
) {
  const startTime = Date.now();
  
  try {
    logger.info(`🛠️ Processing Node: ${JSON.stringify(currentNode, null, 2)}`);

    switch (currentNode.type) {
      case "sendMessage":
        await processSendMessageNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, sendername, username);
        break;

      case "buttons":
        await processButtonsNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username);
        await updateSessionForPause(sender, receiver, flowId, currentNode.id, "buttons");
        return;

      case "list":
        await processListNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username);
        await updateSessionForPause(sender, receiver, flowId, currentNode.id, "list");
        return;

      case "cta_url":
        await processCtaUrlNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
        break;

      case "template":
        await processTemplateNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
        break;

      case "condition":
        await processConditionNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
        break;

      case "location_request_message":
        await processLocationRequestNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username);
        return;

      case "question":
        await processQuestionNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username);
        return;

// Add this to your processNode function's switch statement

case "assignUser":
  await processAssignUserNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
  break;




      case "timeDelay": {
        const delayData = JSON.parse(currentNode.data.delay || '{"minutes":0,"seconds":0}');
        const delayMs = ((delayData.minutes || 0) * 60 + (delayData.seconds || 0)) * 1000;
        if (delayMs > 0) {
          logger.info(`⏳ timeDelay: waiting ${delayMs}ms for node ${currentNode.id}`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
        break;
      }

      // ✅ NEW: Handle AI Node
      case "ai":
        await processAINodeSimple(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
        return;

      default:
        const errorMsg = `Unsupported Node Type: "${currentNode.type}"`;
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'system_error',
          logLevel: 'error',
          message: errorMsg,
          errorDetails: `Node data: ${JSON.stringify(currentNode)}`,
          processingTime: (Date.now() - startTime) / 1000
        });
        throw new Error(errorMsg);
    }

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'flow_started',
      logLevel: 'info',
      message: `Successfully processed ${currentNode.type} node`,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { nodeType: currentNode.type }
    });

  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'system_error',
      logLevel: 'error',
      message: `Failed to process Node ID: ${currentNode.id}, Type: ${currentNode.type}`,
      errorDetails: error.stack,
      processingTime: (Date.now() - startTime) / 1000
    });
    
    logger.error(`❌ Failed to process Node ID: ${currentNode.id}, Type: ${currentNode.type}`);
    throw error;
  }
}

// ✅ NEW: Process AI Node function
async function processAINode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  const startTime = Date.now();
  
  try {
    // Get the user's message from the connected question node
    const userMessage = await getUserResponseFromSession(sender, receiver, flowId);
    
    if (!userMessage) {
      throw new Error("No user message found for AI processing");
    }

    // Get all nodes for the AI service
    const [stepsRows] = await db.query("SELECT nodes FROM chatbot_steps WHERE flow_id = ?", [flowId]);
    
    if (!stepsRows.length) {
      throw new Error(`No steps found for Flow ID: ${flowId}`);
    }

    const nodes = JSON.parse(stepsRows[0].nodes || "[]");

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_node_started',
      logLevel: 'info',
      message: 'AI node processing initiated',
      userMessage,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { 
        connectedQuestionNode: currentNode.data.connectedQuestionNode,
        smartRoutesCount: JSON.parse(currentNode.data.smartRoutes || "[]").length
      }
    });

    // Process with AI service
    await chatbotService.processAINode(
      currentNode,
      userMessage,
      sender,
      receiver,
      flowId,
      apiUrl,
      accessToken,
      sendername,
      username,
      nodes,
      edges
    );

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_node_completed',
      logLevel: 'info',
      message: 'AI node processing completed',
      userMessage,
      processingTime: (Date.now() - startTime) / 1000
    });

  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_node_error',
      logLevel: 'error',
      message: `AI node processing failed: ${error.message}`,
      errorDetails: error.stack,
      processingTime: (Date.now() - startTime) / 1000
    });
    
    logger.error("❌ Error processing AI node:", error.message);
    
    // Send fallback message to user
    const fallbackMessage = currentNode.data.fallbackMessage || "I'm sorry, I couldn't understand that. Please try again.";
    await sendTextMessage(receiver, fallbackMessage, apiUrl, accessToken, sender, flowId, sendername);
  }
}

// Add this function anywhere in your chatbotController.js file:

async function processAINodeSimple(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  const startTime = Date.now();
  
  try {
    // Get user's last response
    const [responseRows] = await db.query(
      `SELECT variable_value FROM chatbot_question 
       WHERE sender_id = ? AND receiver_id = ? AND flow_id = ? 
       ORDER BY created_at DESC LIMIT 1`,
      [sender, receiver, flowId]
    );

    const userMessage = responseRows.length > 0 ? responseRows[0].variable_value : "";
    
    if (!userMessage) {
      logger.error("No user message found for AI processing");
      const fallbackMsg = currentNode.data.fallbackMessage || "Please try again.";
      await sendTextMessage(receiver, fallbackMsg, apiUrl, accessToken, sender, flowId, sendername);
      return;
    }

    logger.info(`AI Processing message: "${userMessage}"`);

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_processing_started',
      message: 'AI processing started',
      userMessage,
      metadata: { userMessage }
    });

    // Parse smart routes
    const smartRoutes = JSON.parse(currentNode.data.smartRoutes || "[]");
    
    // Simple keyword matching
    let matchedRoute = null;
    let bestScore = 0;
    let matchedKeywords = [];

    for (const route of smartRoutes) {
      let score = 0;
      let matchedWords = [];
      
      // Extract words from intentPrompt for matching
      const intentPrompt = route.intentPrompt || "";
      const intentWords = intentPrompt.toLowerCase().split(/[\s,]+/).filter(word => word.length > 2);
      
      logger.info(`🔍 Debug - Route: ${route.id}, Intent: "${intentPrompt}"`);
      logger.info(`🔍 Debug - Extracted words: [${intentWords.join(', ')}]`);
      
      // Also create variations of words (remove hyphens, etc.)
      const intentWordVariations = [];
      for (const word of intentWords) {
        intentWordVariations.push(word);
        // Add variations without hyphens
        if (word.includes('-')) {
          intentWordVariations.push(word.replace(/-/g, ''));
        }
        // Add variations with spaces
        if (word.includes('-')) {
          intentWordVariations.push(word.replace(/-/g, ' '));
        }
      }
      
      for (const word of intentWordVariations) {
        if (userMessage.toLowerCase().includes(word)) {
          score += 1;
          matchedWords.push(word);
        }
      }
      
      // Special handling for common phrases
      const userMessageLower = userMessage.toLowerCase();
      if (intentPrompt.toLowerCase().includes('more details') && 
          (userMessageLower.includes('more') || userMessageLower.includes('details') || userMessageLower.includes('information'))) {
        score += 2; // Give extra points for "more details" intent
        matchedWords.push('more_details_intent');
      }
      
      if (intentPrompt.toLowerCase().includes('connect') && 
          (userMessageLower.includes('connect') || userMessageLower.includes('call') || userMessageLower.includes('speak'))) {
        score += 2; // Give extra points for "connect" intent
        matchedWords.push('connect_intent');
      }
      
      const confidence = intentWords.length > 0 ? score / intentWords.length : 0;
      
      logger.info(`🔍 Debug - Route ${route.id}: score=${score}, confidence=${confidence}, matchedWords=[${matchedWords.join(', ')}]`);
      
      if (confidence > bestScore && confidence >= 0.1) {
        bestScore = confidence;
        matchedRoute = route;
        matchedKeywords = matchedWords;
        logger.info(`✅ Debug - Route ${route.id} selected with confidence ${confidence}`);
      }
    }

    if (!matchedRoute) {
      logger.info("No route matched, generating AI response for general question");
      
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'ai_general_response',
        message: 'No specific route matched, generating general AI response',
        userMessage,
        metadata: { reason: 'general_question' }
      });
      
      // Generate AI response for general questions
      try {
        const generalPrompt = `You are a helpful customer support assistant for CodeCanvas. 
        
User Question: "${userMessage}"
User Name: ${sendername || 'Customer'}

Instructions:
- Provide a helpful, accurate response about CodeCanvas
- Keep response under 1000 characters for WhatsApp
- Be friendly and professional
- If you don't know something, suggest they visit https://codecanvas.org.in/
- Encourage them to ask more specific questions

Response:`;

        const aiResponse = await chatbotService.callAI(generalPrompt, `general_${Date.now()}`);
        
        // Clean up response
        let finalResponse = aiResponse
          .replace(/^(Response:|Here('|')s|Here is)/i, '')
          .replace(/^["\']|["\']$/g, '')
          .trim();
        
        if (finalResponse.length > 1000) {
          finalResponse = finalResponse.substring(0, 997) + "...";
        }
        
        await sendTextMessage(receiver, finalResponse, apiUrl, accessToken, sender, flowId, sendername);
        logger.info(`✅ Generated general AI response: ${finalResponse.substring(0, 100)}...`);
        
      } catch (aiError) {
        logger.warn("⚠️ AI response generation failed:", aiError.message);
        const fallbackMsg = currentNode.data.fallbackMessage || "I'm sorry, I couldn't understand that. Please try again.";
        await sendTextMessage(receiver, fallbackMsg, apiUrl, accessToken, sender, flowId, sendername);
      }
      
      // Keep user in AI node for continued conversation
      await updateSessionSource(sender, receiver, currentNode.id);
      logger.info("✅ User kept in AI node after general response - ready for continued conversation");
      return;
    }

    logger.info(`Route matched: ${matchedRoute.actionType}, confidence: ${bestScore}, keywords: ${matchedKeywords.join(', ')}`);

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_route_matched',
      message: `AI route matched: ${matchedRoute.actionType}`,
      userMessage,
      metadata: {
        actionType: matchedRoute.actionType,
        confidence: bestScore,
        matchedKeywords,
        routeId: matchedRoute.id
      }
    });

    // Execute action
    switch (matchedRoute.actionType) {
      case 'node':
        logger.info(`Redirecting to node: ${matchedRoute.actionConfig.nodeId}`);
        
        await logToPM2({
          username,
          flowId,
          nodeId: matchedRoute.actionConfig.nodeId,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'ai_node_redirect',
          message: `AI redirecting to node: ${matchedRoute.actionConfig.nodeId}`,
          userMessage,
          metadata: { targetNodeId: matchedRoute.actionConfig.nodeId }
        });
        
        // Update session
        await updateSessionSource(sender, receiver, matchedRoute.actionConfig.nodeId);
        
        // Get and process target node
        const [stepsRows] = await db.query("SELECT nodes FROM chatbot_steps WHERE flow_id = ?", [flowId]);
        if (stepsRows.length > 0) {
          const nodes = JSON.parse(stepsRows[0].nodes || "[]");
          const targetNode = nodes.find(node => node.id === matchedRoute.actionConfig.nodeId);
          
          if (targetNode) {
            await processNode(targetNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
          } else {
            logger.error(`Target node ${matchedRoute.actionConfig.nodeId} not found`);
          }
        }
        break;

   case 'chatbot':
  logger.info("Sending AI-powered chatbot response via service");
  
  // Use the chatbot service for proper AI response generation
  await chatbotService.executeChatbotAction(
    matchedRoute, 
    userMessage, 
    receiver, 
    apiUrl, 
    accessToken, 
    sender, 
    flowId, 
    sendername, 
    username
  );
  
  break;

      case 'api':
        logger.info("API action triggered");
        
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'ai_api_triggered',
          message: 'AI API action triggered',
          userMessage,
          metadata: { apiConfig: matchedRoute.actionConfig }
        });
        
        const apiMsg = "I'm fetching that information for you. Please wait...";
        await sendTextMessage(receiver, apiMsg, apiUrl, accessToken, sender, flowId, sendername);
        // TODO: Implement actual API calls later
        break;
    }

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_processing_completed',
      message: 'AI processing completed successfully',
      userMessage,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: {
        actionType: matchedRoute ? matchedRoute.actionType : 'fallback',
        confidence: bestScore,
        matchedKeywords
      }
    });

  } catch (error) {
    logger.error("AI processing error:", error.message);
    
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'ai_processing_error',
      logLevel: 'error',
      message: `AI processing failed: ${error.message}`,
      errorDetails: error.stack,
      processingTime: (Date.now() - startTime) / 1000
    });
    
    const fallbackMsg = currentNode.data.fallbackMessage || "Something went wrong. Please try again.";
    await sendTextMessage(receiver, fallbackMsg, apiUrl, accessToken, sender, flowId, sendername);
  }
}

// ✅ Helper function to get user response from session
async function getUserResponseFromSession(sender, receiver, flowId) {
  try {
    // First check if there's a recent question response
    const [responseRows] = await db.query(
      `SELECT variable_value FROM chatbot_question 
       WHERE sender_id = ? AND receiver_id = ? AND flow_id = ? 
       ORDER BY created_at DESC LIMIT 1`,
      [sender, receiver, flowId]
    );

    if (responseRows.length > 0) {
      return responseRows[0].variable_value;
    }

    // If no stored response, check session for recent user message
    const [sessionRows] = await db.query(
      `SELECT trigger_key FROM chatbot_session 
       WHERE sender_id = ? AND receiver_id = ? 
       ORDER BY updated_at DESC LIMIT 1`,
      [sender, receiver]
    );

    if (sessionRows.length > 0 && sessionRows[0].trigger_key) {
      return sessionRows[0].trigger_key;
    }

    return null;
  } catch (error) {
    logger.error("❌ Error getting user response from session:", error.message);
    return null;
  }
}

/** ========================================================================
 * ✅ NODE PROCESSING FUNCTIONS
 * ======================================================================== */

async function processSendMessageNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, sendername, username) {
  const startTime = Date.now();
  
  try {
    // Process media if present
    if (currentNode.data.mediaType && currentNode.data.mediaFile) {
      const mediaArray = JSON.parse(currentNode.data.mediaFile);
      
      if (mediaArray.length > 0) {
        for (const media of mediaArray) {
          const mediaType = getMediaTypeFromUrl(media.media);
          const fileName = media.name || (mediaType === "document" ? getFilenameFromUrl(media.media) : undefined);
          
          await logToPM2({
            username,
            flowId,
            nodeId: currentNode.id,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'media_sent',
            logLevel: 'info',
            message: `Sending ${mediaType} message`,
            requestPayload: {
              mediaType,
              mediaUrl: media.media,
              caption: currentNode.data.caption,
              fileName
            },
            apiEndpoint: apiUrl
          });

          const mediaRes = await sendMediaMessage(
            receiver,
            currentNode.data.caption || "",
            mediaType,
            media.media,
            apiUrl,
            accessToken,
            fileName
          );
          saveChatMessage({
            username, senderId: sender, receiverId: receiver, senderName: sendername,
            type: mediaType,
            text: currentNode.data.caption || null,
            media: media.media,
            whtsRefId: mediaRes?.messages?.[0]?.id
          });
        }
      }
    }

    // Process text message
    if (currentNode.data.message) {
      const finalMessage = await replaceVariables(currentNode.data.message, sender, receiver, flowId, {}, sendername);
      
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'text_sent',
        logLevel: 'info',
        message: 'Sending text message',
        botResponse: finalMessage,
        requestPayload: {
          type: 'text',
          message: finalMessage
        },
        apiEndpoint: apiUrl,
        processingTime: (Date.now() - startTime) / 1000
      });

      const textRes = await sendTextMessage(receiver, finalMessage, apiUrl, accessToken, sender, flowId, sendername);
      saveChatMessage({
        username, senderId: sender, receiverId: receiver, senderName: sendername,
        type: 'text',
        text: finalMessage,
        whtsRefId: textRes?.messages?.[0]?.id
      });
    }
  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'text_error',
      logLevel: 'error',
      message: 'Failed to send message',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

async function processButtonsNode(node, receiver, apiUrl, accessToken, sender, flowId, sendername, username) {
  const startTime = Date.now();
  
  try {
    const { header, bodyText, footer, buttons } = node.data;
    
    const finalHeader = header ? await replaceVariables(header, sender, receiver, flowId, {}, sendername) : undefined;
    const finalBody = await replaceVariables(bodyText, sender, receiver, flowId, {}, sendername);
    const finalFooter = footer ? await replaceVariables(footer, sender, receiver, flowId, {}, sendername) : undefined;

    const parsedButtons = JSON.parse(buttons).map(async (button) => ({
      type: "reply",
      reply: {
        id: button.id,
        title: await replaceVariables(button.text, sender, receiver, flowId, {}, sendername),
      },
    }));

    const finalButtons = await Promise.all(parsedButtons);

    const buttonPayload = {
      messaging_product: "whatsapp",
      to: receiver,
      type: "interactive",
      interactive: {
        type: "button",
        header: finalHeader ? { type: "text", text: finalHeader } : undefined,
        body: { text: finalBody },
        footer: finalFooter ? { text: finalFooter } : undefined,
        action: { buttons: finalButtons },
      },
    };

    await logToPM2({
      username,
      flowId,
      nodeId: node.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'button_sent',
      logLevel: 'info',
      message: 'Sending button message',
      botResponse: finalBody,
      requestPayload: buttonPayload,
      apiEndpoint: apiUrl,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { buttonCount: finalButtons.length }
    });

    const response = await sendInteractiveMessage(apiUrl, buttonPayload, accessToken);
    saveChatMessage({
      username, senderId: sender, receiverId: receiver, senderName: sendername,
      type: 'button',
      text: JSON.stringify({
        header: finalHeader || null,
        body: finalBody,
        footer: finalFooter || null,
        buttons: finalButtons.map(b => ({ id: b.reply.id, text: b.reply.title }))
      }),
      whtsRefId: response?.messages?.[0]?.id
    });

    await logToPM2({
      username,
      flowId,
      nodeId: node.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'button_sent',
      logLevel: 'info',
      message: 'Button message sent successfully',
      responsePayload: response,
      processingTime: (Date.now() - startTime) / 1000
    });

  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: node.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'button_error',
      logLevel: 'error',
      message: 'Failed to send button message',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

async function processListNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username) {
  const startTime = Date.now();
  
  try {
    const { bodyText, sections } = currentNode.data;
    const finalBodyText = await replaceVariables(bodyText, sender, receiver, flowId, {}, sendername);

    let rawSections = [];
    try {
      rawSections = typeof sections === "string" ? JSON.parse(sections) : sections;
      if (!Array.isArray(rawSections)) throw new Error("Sections is not an array");
    } catch (parseErr) {
      throw new Error("Invalid 'sections' format. Must be an array or valid JSON string.");
    }

    const parsedSections = await Promise.all(
      rawSections.map(async (section, index) => {
        const title = await replaceVariables(section.title || `Section ${index + 1}`, sender, receiver, flowId, {}, sendername);
        
        const rows = await Promise.all(
          (section.rows || []).map(async (row) => ({
            id: row.id,
            title: await replaceVariables(row.text || "", sender, receiver, flowId, {}, sendername),
            description: row.description
              ? await replaceVariables(row.description, sender, receiver, flowId, {}, sendername)
              : "",
          }))
        );

        return { title, rows };
      })
    );

    if (!parsedSections.length || parsedSections.every(sec => sec.rows.length === 0)) {
      throw new Error("No rows available to show in the list.");
    }

    const listPayload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to: receiver,
      type: "interactive",
      interactive: {
        type: "list",
        body: { text: finalBodyText },
        footer: { text: "Please choose one" },
        action: {
          button: "Select",
          sections: parsedSections,
        },
      },
    };

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'list_sent',
      logLevel: 'info',
      message: 'Sending list message',
      botResponse: finalBodyText,
      requestPayload: listPayload,
      apiEndpoint: apiUrl,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { sectionCount: parsedSections.length }
    });

    const headers = getHeaders(apiUrl, accessToken);
    const response = await axios.post(apiUrl, listPayload, { headers });
    saveChatMessage({
      username, senderId: sender, receiverId: receiver, senderName: sendername,
      type: 'list',
      text: JSON.stringify({
        body: finalBodyText,
        sections: parsedSections.map(sec => ({
          title: sec.title,
          rows: sec.rows.map(r => ({ id: r.id, text: r.title }))
        }))
      }),
      whtsRefId: response?.data?.messages?.[0]?.id
    });

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'list_sent',
      logLevel: 'info',
      message: 'List message sent successfully',
      responsePayload: response.data,
      processingTime: (Date.now() - startTime) / 1000
    });

    return response.data;
  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'list_error',
      logLevel: 'error',
      message: 'Failed to send list message',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

async function processQuestionNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username) {
  const startTime = Date.now();
  
  try {
    const variableName = currentNode.data.variable || null;
    let questionType = "text";

    if (!currentNode.data.answerOptions || currentNode.data.answerOptions === "[]") {
      const message = currentNode.data.label;
      const finalMessage = await replaceVariables(message, sender, receiver, flowId, {}, sendername);
      
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'question_sent',
        logLevel: 'info',
        message: 'Sending text question',
        botResponse: finalMessage,
        metadata: { questionType: 'text', variableName }
      });

      const qTextRes = await sendTextMessage(receiver, finalMessage, apiUrl, accessToken, sender, flowId, sendername);
      saveChatMessage({
        username, senderId: sender, receiverId: receiver, senderName: sendername,
        type: 'text',
        text: finalMessage,
        whtsRefId: qTextRes?.messages?.[0]?.id
      });
    } else {
      const payload = await buildButtonPayload(
        receiver,
        null,
        currentNode.data.label,
        null,
        currentNode.data.answerOptions,
        sender,
        receiver,
        flowId,
        sendername
      );

      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'question_sent',
        logLevel: 'info',
        message: 'Sending button question',
        botResponse: currentNode.data.label,
        requestPayload: payload,
        metadata: { questionType: 'button', variableName }
      });

      const qBtnRes = await sendInteractiveMessage(apiUrl, payload, accessToken);
      const answerOptions = JSON.parse(currentNode.data.answerOptions || '[]');
      saveChatMessage({
        username, senderId: sender, receiverId: receiver, senderName: sendername,
        type: 'button',
        text: JSON.stringify(answerOptions.map(b => ({ id: b.id, text: b.text }))),
        whtsRefId: qBtnRes?.messages?.[0]?.id
      });
      questionType = "button";
    }

    await updateSessionForPause(
      sender,
      receiver,
      flowId,
      currentNode.id,
      "question",
      questionType,
      variableName
    );

  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'question_error',
      logLevel: 'error',
      message: 'Failed to send question',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}


async function processTemplateNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  const startTime = Date.now();

  try {
    logger.info("🐛 DEBUG - processTemplateNode called with:");
    logger.info(`🐛 currentNode.data: ${JSON.stringify(currentNode.data, null, 2)}`);
    logger.info(`🐛 username: ${username}`);
    logger.info(`🐛 templateName: ${currentNode.data.templateName}`);

    const templateName = currentNode.data.templateName;

    const [templateRows] = await db.query(
      "SELECT * FROM templates WHERE template_name = ? AND username = ?",
      [templateName, username]
    );

    logger.info(`🐛 Template query results: ${templateRows.length} rows found`);

    if (templateRows.length === 0) {
      logger.error(`❌ Template not found for template_name: ${templateName} and username: ${username}`);

      const [allTemplates] = await db.query(
        "SELECT template_name FROM templates WHERE username = ?",
        [username]
      );
      logger.info(`🐛 Available templates for ${username}: ${JSON.stringify(allTemplates.map(t => t.template_name))}`);

      throw new Error(`Template not found: ${templateName}`);
    }

    const template = templateRows[0];
    const flowToken = template.delete2;
    logger.info(`🐛 Flow token from delete2: "${flowToken}"`);

    const [languageData] = await db.query(
      "SELECT short_name FROM languages WHERE id = ?",
      [template.language]
    );

    if (!languageData.length) {
      throw new Error(`Language not found for ID: ${template.language}`);
    }

    const languageCode = languageData[0].short_name;

    const payload = {
      messaging_product: "whatsapp",
      to: receiver,
      type: "template",
      template: {
        name: template.template_name,
        language: { code: languageCode },
        components: [],
      },
    };

    const rawAttributes = JSON.parse(currentNode.data.attributes || "[]");
    if (rawAttributes.length > 0) {
      const textParams = await Promise.all(
        rawAttributes
          .filter(attr => typeof attr === "string" && attr.trim() !== "")
          .map(async attr => {
            const resolvedAttr = await replaceVariables(attr, sender, receiver, flowId, {}, sendername);
            return { type: "text", text: resolvedAttr };
          })
      );

      if (textParams.length > 0) {
        payload.template.components.push({
          type: "body",
          parameters: textParams,
        });
      }
    }

    const { headerMediaType, mediaPreview } = currentNode.data;
    if (headerMediaType && mediaPreview) {
      const fileType = (() => {
        const ext = mediaPreview.split(".").pop().toLowerCase();
        if (["jpg", "jpeg", "png"].includes(ext)) return "image";
        if (["pdf", "doc", "docx"].includes(ext)) return "document";
        if (["mp4", "mov", "avi"].includes(ext)) return "video";
        return "image";
      })();

      payload.template.components.push({
        type: "header",
        parameters: [
          {
            type: fileType,
            [fileType]: { link: mediaPreview },
          },
        ],
      });
    }

    // 🔍 Specific condition: if delete2 === "flow"
    if (flowToken?.trim().toLowerCase() === "flow") {
      logger.info(`🔄 ADDING FLOW COMPONENT using static FLOW_TOKEN`);
      const flowComponent = {
        type: "button",
        sub_type: "flow",
        index: 0,
        parameters: [
          {
            type: "action",
            action: {
              flow_token: "FLOW_TOKEN", // 👈 static token
              flow_action_data: [],
            },
          },
        ],
      };

      payload.template.components.push(flowComponent);
      logger.info(`🔄 Flow component added (from delete2 === "flow"): ${JSON.stringify(flowComponent, null, 2)}`);
    } else if (flowToken && flowToken.trim() !== "" && flowToken.toLowerCase() !== "null") {
      logger.info(`🔄 ADDING FLOW COMPONENT with token: ${flowToken.trim()}`);
      const flowComponent = {
        type: "button",
        sub_type: "flow",
        index: 0,
        parameters: [
          {
            type: "action",
            action: {
              flow_token: flowToken.trim(),
              flow_action_data: [],
            },
          },
        ],
      };

      payload.template.components.push(flowComponent);
      logger.info(`🔄 Flow component added: ${JSON.stringify(flowComponent, null, 2)}`);
    } else {
      logger.info(`❌ No valid flow token found, skipping flow component`);
    }

    logger.info(`🐛 Final payload: ${JSON.stringify(payload, null, 2)}`);

    const sendRes = await sendInteractiveMessage(apiUrl, payload, accessToken);
    logger.info("✅ Template message sent successfully");

    // Build attributes object: {attribute1: val, attribute2: val, ...} or [] if none
    const resolvedAttrValues = await Promise.all(
      rawAttributes.map(attr => attr && attr.trim()
        ? replaceVariables(attr, sender, receiver, flowId, {}, sendername)
        : Promise.resolve('')
      )
    );
    const attributesForChat = rawAttributes.length > 0
      ? Object.fromEntries(resolvedAttrValues.map((v, i) => [`attribute${i + 1}`, v]))
      : [];

    saveChatMessage({
      username, senderId: sender, receiverId: receiver, senderName: sendername,
      type: 'template',
      text: template.template_name,
      whtsRefId: sendRes?.messages?.[0]?.id,
      templateId: template.id,
      attributes: attributesForChat,
      templateMedia: mediaPreview || null
    });

    return sendRes;
  } catch (error) {
    logger.error(`❌ Error in processTemplateNode: ${error.message}`);
    logger.error(`❌ Stack trace: ${error.stack}`);
    throw error;
  }
}



async function processAssignUserNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  const startTime = Date.now();
  
  try {
    const agentData = currentNode.data;
    // Handle different data structures - check for agentUsername or user field
    const agentUsername = agentData.agentUsername || agentData.user;
    
    if (!agentUsername) {
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'assign_user_error',
        logLevel: 'warning',
        message: 'No agent username found in node data, skipping assignment',
        metadata: { nodeData: agentData }
      });
      
      // Skip to next node without error message - but don't process it here
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'assign_user_error',
        logLevel: 'warning',
        message: `No assign_users record found for username: ${agentUsername}, assignment skipped`,
        metadata: { agentUsername }
      });
      return; // Let main flow loop handle next node
    }
    
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'assign_user_started',
      logLevel: 'info',
      message: `Starting user assignment to agent: ${agentUsername}`,
      metadata: { 
        agentUsername,
        agentName: agentData.agentName,
        agentEmail: agentData.agentEmail,
        agentMobile: agentData.agentMobile
      }
    });

    // Step 1: Get assign_users record for the agent
    const [assignUserRows] = await db.query(
      "SELECT id FROM assign_users WHERE assign_user = ?", 
      [agentUsername]
    );

    if (assignUserRows.length === 0) {
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'assign_user_error',
        logLevel: 'warning',
        message: `No assign_users record found for username: ${agentUsername}, skipping assignment`,
        metadata: { agentUsername }
      });
      
      // Skip assignment but let main flow continue
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'assign_user_error',
        logLevel: 'warning',
        message: 'No agent username found in node data, assignment skipped',
        metadata: { nodeData: agentData }
      });
      return; // Let main flow loop handle next node
    }

    const assignUserId = assignUserRows[0].id;

    // Step 2: Check if chat_messages_room exists for sender and receiver
    const [chatMessagesRows] = await db.query(
      "SELECT id, assign_to FROM chat_messages_room WHERE sender_id = ? AND receiver_id = ?",
      [sender, receiver]
    );

    let isNewAssignment = false; // Track if this is a new assignment

    if (chatMessagesRows.length === 0) {
      // Create new chat_messages_room entry if it doesn't exist
      await db.query(
        `INSERT INTO chat_messages_room 
         (sender_id, receiver_id, assign_to, created_at, updated_at) 
         VALUES (?, ?, ?, NOW(), NOW())`,
        [sender, receiver, assignUserId]
      );
      
      isNewAssignment = true; // This is a new assignment
      
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'chat_room_created',
        logLevel: 'info',
        message: `New chat room created and assigned to agent ID: ${assignUserId}`,
        metadata: { assignUserId, agentUsername, isNewAssignment }
      });
    } else {
      const chatMessage = chatMessagesRows[0];
      
      if (chatMessage.assign_to) {
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'assign_user_skipped',
          logLevel: 'info',
          message: `Chat already assigned to agent ID: ${chatMessage.assign_to}, skipping template notification`,
          metadata: { existingAssignTo: chatMessage.assign_to, newAssignUserId: assignUserId, isNewAssignment: false }
        });
      } else {
        // Update assign_to field
        await db.query(
          "UPDATE chat_messages_room SET assign_to = ?, updated_at = NOW() WHERE id = ?",
          [assignUserId, chatMessage.id]
        );
        
        isNewAssignment = true; // This is a new assignment (was unassigned before)
        
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'assign_user_updated',
          logLevel: 'info',
          message: `Chat assigned to agent ID: ${assignUserId}`,
          metadata: { assignUserId, agentUsername, chatRoomId: chatMessage.id, isNewAssignment }
        });
      }
    }

    // Step 3: Handle template notification if enabled AND this is a new assignment
    const enableNotification = agentData.enableNotification === "true";
    
    if (enableNotification && agentData.templateName && agentData.templateBody && isNewAssignment) {
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_notification_started',
        logLevel: 'info',
        message: 'Template notification enabled for new assignment, checking credits and sending template',
        metadata: { 
          templateName: agentData.templateName,
          templateCategory: agentData.templateCategory || "1",
          enableNotification: true,
          isNewAssignment: true
        }
      });

      try {
        // Determine credit type based on template category
        const templateCategory = agentData.templateCategory || "1"; // Default to category 1
        let creditColumn, creditType;
        
        switch (templateCategory) {
          case "1":
            creditColumn = "whatsapp_marketing_credits";
            creditType = "marketing";
            break;
          case "2":
            creditColumn = "whatsapp_utility_credits";
            creditType = "utility";
            break;
          case "3":
            creditColumn = "whatsapp_credits";
            creditType = "whatsapp";
            break;
          default:
            creditColumn = "whatsapp_marketing_credits";
            creditType = "marketing";
            break;
        }
        
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'debug_credit_query_start',
          logLevel: 'info',
          message: 'Starting credit query execution',
          metadata: { 
            username, 
            queryType: 'SELECT_CREDITS',
            templateCategory,
            creditColumn,
            creditType
          }
        });
        
        const [creditRows] = await db.query(
          `SELECT ${creditColumn} FROM credits WHERE username = ?`,
          [username]
        );
        
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'debug_credit_query_success',
          logLevel: 'info',
          message: 'Credit query executed successfully',
          metadata: { 
            username, 
            creditRowsCount: creditRows.length, 
            queryType: 'SELECT_CREDITS',
            templateCategory,
            creditColumn,
            creditType
          }
        });

        if (creditRows.length === 0) {
          await logToPM2({
            username,
            flowId,
            nodeId: currentNode.id,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'template_credit_error',
            logLevel: 'warning',
            message: 'No credit record found for user, skipping template',
            metadata: { username }
          });
        } else {
          const currentCredits = creditRows[0][creditColumn];
          
          if (currentCredits <= 0) {
            await logToPM2({
              username,
              flowId,
              nodeId: currentNode.id,
              senderId: sender,
              receiverId: receiver,
              senderName: sendername,
              logType: 'template_credit_insufficient',
              logLevel: 'warning',
              message: 'Insufficient credits, skipping template notification',
              metadata: { 
                currentCredits, 
                requiredCredits: 1,
                templateCategory,
                creditType,
                creditColumn
              }
            });
          } else {
            // Deduct 1 credit
            const newCredits = currentCredits - 1;
            
            await logToPM2({
              username,
              flowId,
              nodeId: currentNode.id,
              senderId: sender,
              receiverId: receiver,
              senderName: sendername,
              logType: 'debug_credit_update_start',
              logLevel: 'info',
              message: 'Starting credit update execution',
              metadata: { 
                username, 
                currentCredits, 
                newCredits, 
                queryType: 'UPDATE_CREDITS',
                templateCategory,
                creditType,
                creditColumn
              }
            });
            
            await db.query(
              `UPDATE credits SET ${creditColumn} = ? WHERE username = ?`,
              [newCredits, username]
            );
            
            await logToPM2({
              username,
              flowId,
              nodeId: currentNode.id,
              senderId: sender,
              receiverId: receiver,
              senderName: sendername,
              logType: 'debug_credit_update_success',
              logLevel: 'info',
              message: 'Credit update executed successfully',
              metadata: { 
                username, 
                currentCredits, 
                newCredits, 
                queryType: 'UPDATE_CREDITS',
                templateCategory,
                creditType,
                creditColumn
              }
            });

            await logToPM2({
              username,
              flowId,
              nodeId: currentNode.id,
              senderId: sender,
              receiverId: receiver,
              senderName: sendername,
              logType: 'template_credit_deducted',
              logLevel: 'info',
              message: `${creditType} credit deducted: ${currentCredits} -> ${newCredits}`,
              metadata: { 
                previousCredits: currentCredits,
                newCredits: newCredits,
                deductedAmount: 1,
                templateCategory,
                creditType,
                creditColumn
              }
            });

            // Send template to agent
            const templateResult = await sendTemplateToAgent(
              agentData,
              sendername,
              receiver,
              apiUrl,
              accessToken,
              username,
              flowId,
              currentNode.id,
              sender
            );

            // If template sending failed, send a fallback text message
            if (!templateResult) {
              await logToPM2({
                username,
                flowId,
                nodeId: currentNode.id,
                senderId: sender,
                receiverId: receiver,
                senderName: sendername,
                logType: 'template_fallback_sending',
                logLevel: 'info',
                message: 'Template failed, sending fallback text message to agent',
                metadata: { 
                  agentMobile: agentData.agentMobile,
                  templateName: agentData.templateName
                }
              });

              const fallbackMessage = `New chat assigned to you from ${sendername} (${receiver}). Please check your dashboard.`;
              await sendTextMessage(
                agentData.agentMobile,
                fallbackMessage,
                apiUrl,
                accessToken,
                sender,
                flowId,
                sendername
              );

              await logToPM2({
                username,
                flowId,
                nodeId: currentNode.id,
                senderId: sender,
                receiverId: receiver,
                senderName: sendername,
                logType: 'template_fallback_sent',
                logLevel: 'info',
                message: 'Fallback text message sent to agent',
                botResponse: fallbackMessage,
                metadata: { 
                  agentMobile: agentData.agentMobile,
                  fallbackMessage
                }
              });
            }
          }
        }
      } catch (templateError) {
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'debug_error_details',
          logLevel: 'error',
          message: `Detailed error information: ${templateError.message}`,
          errorDetails: templateError.stack,
          metadata: { 
            templateName: agentData.templateName,
            errorName: templateError.name,
            errorCode: templateError.code,
            errorSqlState: templateError.sqlState,
            errorSqlMessage: templateError.sqlMessage,
            errorStack: templateError.stack
          }
        });
        
        await logToPM2({
          username,
          flowId,
          nodeId: currentNode.id,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'template_error',
          logLevel: 'error',
          message: `Template sending failed: ${templateError.message}`,
          errorDetails: templateError.stack,
          metadata: { templateName: agentData.templateName }
        });
        
        logger.error("❌ Error sending template to agent:", templateError.message);
      }
    } else {
      await logToPM2({
        username,
        flowId,
        nodeId: currentNode.id,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_notification_skipped',
        logLevel: 'info',
        message: 'Template notification disabled, template data missing, or user already assigned',
        metadata: { 
          enableNotification,
          hasTemplateName: !!agentData.templateName,
          hasTemplateBody: !!agentData.templateBody,
          isNewAssignment
        }
      });
    }

    // Step 4: Assignment completed - do NOT process next node here
    // Let the main flow loop handle the next node to avoid duplication
    
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'assign_user_completed',
      logLevel: 'info',
      message: 'User assignment process completed successfully',
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { 
        agentUsername,
        assignUserId,
        nextNodeWillBeProcessedByMainLoop: true
      }
    });

  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'assign_user_error',
      logLevel: 'error',
      message: `User assignment failed: ${error.message}`,
      errorDetails: error.stack,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { 
        agentUsername: currentNode.data.agentUsername || currentNode.data.user,
        errorType: error.name
      }
    });
    
    logger.error("❌ Error in processAssignUserNode:", error.message);
    
    // Skip to next node without sending error message to user
    await processNextNodeSilently(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username);
  }
}

// Helper function to send template to agent
// Enhanced sendTemplateToAgent with comprehensive PM2 logging
async function sendTemplateToAgent(agentData, sendername, receiver, apiUrl, accessToken, username, flowId, nodeId, sender) {
  const startTime = Date.now();
  
  try {
    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_agent_process_start',
      logLevel: 'info',
      message: 'Starting template agent processing',
      metadata: {
        templateName: agentData.templateName,
        agentMobile: agentData.agentMobile,
        hasTemplateVariables: !!(agentData.templateVariables && agentData.templateVariables !== "{}"),
        hasTemplateAttributes: !!(agentData.templateAttributes && agentData.templateAttributes !== "[]"),
        rawTemplateVariables: agentData.templateVariables,
        rawTemplateAttributes: agentData.templateAttributes
      }
    });

    // Get agent's mobile number for sending the template
    const agentMobile = agentData.agentMobile;
    
    if (!agentMobile) {
      await logToPM2({
        username,
        flowId,
        nodeId,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_agent_mobile_missing',
        logLevel: 'warning',
        message: 'Agent mobile number not found, cannot send template',
        metadata: { agentData: JSON.stringify(agentData) }
      });
      return null;
    }

    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_agent_mobile_found',
      logLevel: 'info',
      message: `Agent mobile number found: ${agentMobile}`,
      metadata: { agentMobile }
    });

    // Fetch template language from database
    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_language_query_start',
      logLevel: 'info',
      message: 'Starting template language query',
      metadata: { 
        templateName: agentData.templateName,
        username
      }
    });

    const [templateRows] = await db.query(
      "SELECT language FROM templates WHERE template_name = ? AND username = ?",
      [agentData.templateName, username]
    );

    let languageCode = "en"; // Default fallback
    if (templateRows.length > 0) {
      const template = templateRows[0];
      const [languageData] = await db.query(
        "SELECT short_name FROM languages WHERE id = ?",
        [template.language]
      );
      
      if (languageData.length > 0) {
        languageCode = languageData[0].short_name;
      }

      await logToPM2({
        username,
        flowId,
        nodeId,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_language_found',
        logLevel: 'info',
        message: `Template language resolved: ${languageCode}`,
        metadata: { 
          templateName: agentData.templateName,
          languageId: template.language,
          languageCode,
          templateRowsFound: templateRows.length
        }
      });
    } else {
      await logToPM2({
        username,
        flowId,
        nodeId,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_language_not_found',
        logLevel: 'warning',
        message: `Template not found in database, using default language: ${languageCode}`,
        metadata: { 
          templateName: agentData.templateName,
          username,
          defaultLanguage: languageCode
        }
      });
    }

    // Build template payload
    const templatePayload = {
      messaging_product: "whatsapp",
      to: agentMobile,
      type: "template",
      template: {
        name: agentData.templateName,
        language: { code: languageCode },
        components: []
      }
    };

    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_payload_initialized',
      logLevel: 'info',
      message: 'Template payload initialized',
      metadata: {
        templateName: agentData.templateName,
        languageCode,
        agentMobile
      }
    });

    // Handle template variables - Enhanced logging
    let hasBodyParameters = false;
    
    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_variable_processing_start',
      logLevel: 'info',
      message: 'Starting template variable processing',
      metadata: {
        templateVariables: agentData.templateVariables,
        templateAttributes: agentData.templateAttributes,
        sendername,
        receiver
      }
    });
    
    // Check if we have templateVariables (new format)
    if (agentData.templateVariables && agentData.templateVariables !== "{}") {
      await logToPM2({
        username,
        flowId,
        nodeId,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_variables_found',
        logLevel: 'info',
        message: 'Template variables found, processing...',
        metadata: {
          rawTemplateVariables: agentData.templateVariables
        }
      });

      try {
        const templateVariables = JSON.parse(agentData.templateVariables);
        const variableKeys = Object.keys(templateVariables);
        
        await logToPM2({
          username,
          flowId,
          nodeId,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'template_variables_parsed',
          logLevel: 'info',
          message: 'Template variables parsed successfully',
          metadata: {
            templateVariables: templateVariables,
            variableKeys: variableKeys,
            variableCount: variableKeys.length
          }
        });
        
        if (variableKeys.length > 0) {
          const textParams = [];
          
          for (const key of variableKeys) {
            const variableMapping = templateVariables[key]; // e.g., "@name" or "@number"
            let resolvedValue = variableMapping;
            
            await logToPM2({
              username,
              flowId,
              nodeId,
              senderId: sender,
              receiverId: receiver,
              senderName: sendername,
              logType: 'template_variable_processing',
              logLevel: 'info',
              message: `Processing variable: ${key} = ${variableMapping}`,
              metadata: {
                variableKey: key,
                variableMapping: variableMapping,
                originalValue: resolvedValue
              }
            });
            
            // Replace @name with sendername
            if (variableMapping === '@name') {
              resolvedValue = sendername || 'Unknown User';
              await logToPM2({
                username,
                flowId,
                nodeId,
                senderId: sender,
                receiverId: receiver,
                senderName: sendername,
                logType: 'template_variable_name_replaced',
                logLevel: 'info',
                message: `Variable @name replaced with: ${resolvedValue}`,
                metadata: {
                  variableKey: key,
                  originalValue: variableMapping,
                  resolvedValue: resolvedValue,
                  sendername: sendername
                }
              });
            }
            // Replace @number with receiver number
            else if (variableMapping === '@number') {
              resolvedValue = receiver;
              await logToPM2({
                username,
                flowId,
                nodeId,
                senderId: sender,
                receiverId: receiver,
                senderName: sendername,
                logType: 'template_variable_number_replaced',
                logLevel: 'info',
                message: `Variable @number replaced with: ${resolvedValue}`,
                metadata: {
                  variableKey: key,
                  originalValue: variableMapping,
                  resolvedValue: resolvedValue,
                  receiver: receiver
                }
              });
            }
            // Handle other @ variables by removing the @
            else if (variableMapping.startsWith('@')) {
              resolvedValue = variableMapping.substring(1);
              await logToPM2({
                username,
                flowId,
                nodeId,
                senderId: sender,
                receiverId: receiver,
                senderName: sendername,
                logType: 'template_variable_generic_replaced',
                logLevel: 'info',
                message: `Variable ${variableMapping} processed as: ${resolvedValue}`,
                metadata: {
                  variableKey: key,
                  originalValue: variableMapping,
                  resolvedValue: resolvedValue
                }
              });
            }
            
            textParams.push({ type: "text", text: resolvedValue });
          }

          templatePayload.template.components.push({
            type: "body",
            parameters: textParams
          });
          hasBodyParameters = true;
          
          await logToPM2({
            username,
            flowId,
            nodeId,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'template_variables_processed_success',
            logLevel: 'info',
            message: 'Template variables processed and added to payload',
            metadata: {
              processedParameters: textParams,
              parameterCount: textParams.length,
              hasBodyParameters: true
            }
          });
        } else {
          await logToPM2({
            username,
            flowId,
            nodeId,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'template_variables_empty',
            logLevel: 'info',
            message: 'Template variables object is empty',
            metadata: {
              templateVariables: templateVariables
            }
          });
        }
      } catch (parseError) {
        await logToPM2({
          username,
          flowId,
          nodeId,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'template_variables_parse_error',
          logLevel: 'error',
          message: `Failed to parse template variables: ${parseError.message}`,
          errorDetails: parseError.stack,
          metadata: { 
            templateVariables: agentData.templateVariables,
            errorName: parseError.name
          }
        });
        logger.warn("Failed to parse template variables:", parseError.message);
      }
    } else {
      await logToPM2({
        username,
        flowId,
        nodeId,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_variables_not_found',
        logLevel: 'info',
        message: 'No template variables found, checking template attributes',
        metadata: {
          templateVariables: agentData.templateVariables
        }
      });
    }
    
    // Fallback to old templateAttributes format if no variables were processed
    if (!hasBodyParameters && agentData.templateAttributes && agentData.templateAttributes !== "[]") {
      await logToPM2({
        username,
        flowId,
        nodeId,
        senderId: sender,
        receiverId: receiver,
        senderName: sendername,
        logType: 'template_attributes_fallback_start',
        logLevel: 'info',
        message: 'Processing template attributes as fallback',
        metadata: {
          templateAttributes: agentData.templateAttributes
        }
      });

      try {
        const templateAttributes = JSON.parse(agentData.templateAttributes);
        
        if (templateAttributes.length > 0) {
          const textParams = templateAttributes
            .filter(attr => typeof attr === "string" && attr.trim() !== "")
            .map(attr => {
              let processedAttr = attr;
              if (processedAttr.includes('@name')) {
                processedAttr = processedAttr.replace(/@name/g, sendername || 'Unknown User');
              }
              if (processedAttr.includes('@number')) {
                processedAttr = processedAttr.replace(/@number/g, receiver);
              }
              return { type: "text", text: processedAttr };
            });

          if (textParams.length > 0) {
            templatePayload.template.components.push({
              type: "body",
              parameters: textParams
            });
            hasBodyParameters = true;
          }

          await logToPM2({
            username,
            flowId,
            nodeId,
            senderId: sender,
            receiverId: receiver,
            senderName: sendername,
            logType: 'template_attributes_processed_success',
            logLevel: 'info',
            message: 'Template attributes processed successfully',
            metadata: {
              templateAttributes: templateAttributes,
              processedParameters: textParams,
              parameterCount: textParams.length
            }
          });
        }
      } catch (parseError) {
        await logToPM2({
          username,
          flowId,
          nodeId,
          senderId: sender,
          receiverId: receiver,
          senderName: sendername,
          logType: 'template_attributes_parse_error',
          logLevel: 'error',
          message: `Failed to parse template attributes: ${parseError.message}`,
          errorDetails: parseError.stack,
          metadata: { 
            templateAttributes: agentData.templateAttributes
          }
        });
        logger.warn("Failed to parse template attributes:", parseError.message);
      }
    }

    // Final payload preparation
    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_final_payload_prepared',
      logLevel: 'info',
      message: 'Final template payload prepared for sending',
      requestPayload: templatePayload,
      metadata: {
        agentMobile,
        templateName: agentData.templateName,
        hasBodyParameters,
        componentCount: templatePayload.template.components.length,
        languageCode
      }
    });

    // Send the template message
    const headers = getHeaders(apiUrl, accessToken);
    
    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_api_call_start',
      logLevel: 'info',
      message: 'Starting API call to send template',
      requestPayload: templatePayload,
      apiEndpoint: apiUrl,
      metadata: {
        agentMobile,
        templateName: agentData.templateName,
        languageCode,
        headerCount: Object.keys(headers).length
      }
    });
    
    const response = await axios.post(apiUrl, templatePayload, { headers });

    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_sent_to_agent_success',
      logLevel: 'info',
      message: 'Template successfully sent to agent',
      responsePayload: response.data,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: {
        agentMobile,
        templateName: agentData.templateName,
        messageId: response.data.messages?.[0]?.id,
        responseStatus: response.status,
        responseStatusText: response.statusText
      }
    });

    logger.info(`✅ Template sent to agent ${agentMobile}: ${agentData.templateName}`);
    return response.data;

  } catch (error) {
    // Enhanced error logging with more details
    const errorDetails = {
      message: error.message,
      status: error.response?.status,
      statusText: error.response?.statusText,
      responseData: error.response?.data,
      requestUrl: error.config?.url,
      requestMethod: error.config?.method,
      requestData: error.config?.data,
      errorName: error.name,
      errorCode: error.code
    };

    await logToPM2({
      username,
      flowId,
      nodeId,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'template_send_to_agent_error',
      logLevel: 'error',
      message: `Failed to send template to agent: ${error.message}`,
      errorDetails: JSON.stringify(errorDetails),
      processingTime: (Date.now() - startTime) / 1000,
      metadata: {
        agentMobile: agentData.agentMobile,
        templateName: agentData.templateName,
        errorStatus: error.response?.status,
        errorResponse: error.response?.data,
        fullErrorDetails: errorDetails
      }
    });
    
    logger.error("❌ Error sending template to agent:", error.message);
    logger.error("❌ Error details:", JSON.stringify(errorDetails, null, 2));
    
    return null;
  }
}
// Helper function to process next node silently
async function processNextNodeSilently(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  try {
    const nextEdge = edges.find((edge) => edge.source === currentNode.id);
    if (nextEdge) {
      const [stepsRows] = await db.query("SELECT nodes FROM chatbot_steps WHERE flow_id = ?", [flowId]);
      
      if (stepsRows.length > 0) {
        const nodes = JSON.parse(stepsRows[0].nodes || "[]");
        const nextNode = nodes.find((node) => node.id === nextEdge.target);
        
        if (nextNode) {
          await processNode(
            nextNode,
            receiver,
            apiUrl,
            accessToken,
            flowId,
            sender,
            edges,
            sendername,
            username
          );
        }
      }
    }
  } catch (error) {
    logger.error("❌ Error processing next node silently:", error.message);
  }
}

/** ========================================================================
 * ✅ UTILITY FUNCTIONS
 * ======================================================================== */

async function handleSession(sender, receiver, source, flowId, messageType, triggerKey, sourceHandles) {
  try {
    await logToPM2({
      username: 'system',
      flowId,
      nodeId: source,
      senderId: sender,
      receiverId: receiver,
      logType: messageType === 'trigger' ? 'session_created' : 'session_updated',
      logLevel: 'info',
      message: `Session ${messageType === 'trigger' ? 'created' : 'updated'} for flow: ${flowId}`,
      sessionData: { source, messageType, triggerKey },
      metadata: { sourceHandles: sourceHandles ? sourceHandles.length : 0 }
    });

    const sourceHandlesStr = sourceHandles ? JSON.stringify(sourceHandles) : "[]";

    const [existingSessions] = await db.query(
      `SELECT id, source, message_type, trigger_key, sourceHandle 
       FROM chatbot_session 
       WHERE sender_id = ? AND receiver_id = ?
       ORDER BY updated_at DESC
       LIMIT 1`, 
      [sender, receiver]
    );

    if (existingSessions.length > 0) {
      const existingSession = existingSessions[0];
      if (existingSession.trigger_key === triggerKey) {
        const expiryTime = new Date(Date.now() + 1 * 60 * 60 * 1000);
        await db.query(
          `UPDATE chatbot_session 
           SET source = ?, message_type = ?, sourceHandle = ?, expiry_time = ?, updated_at = NOW() 
           WHERE id = ?`,
          [source, messageType, sourceHandlesStr, expiryTime, existingSession.id]
        );
        return;
      }
    }

    const expiryTime = new Date(Date.now() + 1 * 60 * 60 * 1000);
    await db.query(
      `INSERT INTO chatbot_session 
       (sender_id, receiver_id, flow_id, source, message_type, trigger_key, sourceHandle, expiry_time, created_at, updated_at) 
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
      [sender, receiver, flowId, source, messageType, triggerKey, sourceHandlesStr, expiryTime]
    );
  } catch (error) {
    logger.error(`❌ Failed to handle session: [${error.code || 'ERR'}] ${error.sqlMessage || error.message}`);
    logger.error("Stack:", error.stack);
    throw error;
  }
}

async function updateSessionForPause(sender, receiver, flowId, nodeId, messageType, questionType, variableName) {
  try {
    const expiryTime = new Date(Date.now() + 1 * 60 * 60 * 1000);
    await db.query(
      `UPDATE chatbot_session
       SET source = ?, message_type = ?, question_type = ?, variable_name = ?, expiry_time = ?, updated_at = NOW()
       WHERE sender_id = ? AND receiver_id = ?`,
      [nodeId, messageType, questionType, variableName, expiryTime, sender, receiver]
    );

    logger.info(`✅ Session updated for pause. Sender: ${sender}, Flow ID: ${flowId}, Node ID: ${nodeId}`);
  } catch (error) {
    logger.error("❌ Failed to update session for pause:", error.message);
    throw error;
  }
}

async function storeUserResponse(sender, receiver, flowId, variableName, variableValue, username, nodeData = {}) {
  try {
    let storedValue = variableValue;
    let buttonText = null;
    let listText = null;
    let interactionType = 'question';

    // Check if the value is a button or list selection ID (numeric or string ID)
    if (!isNaN(variableValue) || (typeof variableValue === 'string' && variableValue.length < 50)) {
      // Try to get button text first
      buttonText = await getButtonTextById(variableValue, flowId);
      if (buttonText) {
        storedValue = buttonText;
        interactionType = 'button';
        logger.info(`🔘 Button selection detected: ${variableValue} -> ${buttonText}`);
      } else {
        // If not a button, try to get list item text
        listText = await getListTextById(variableValue, flowId);
        if (listText) {
          storedValue = listText;
          interactionType = 'list';
          logger.info(`📝 List selection detected: ${variableValue} -> ${listText}`);
        }
      }
    }

    await logToPM2({
      username,
      flowId,
      senderId: sender,
      receiverId: receiver,
      logType: 'variable_stored',
      logLevel: 'info',
      message: `User response stored: ${variableName} = ${storedValue} (type: ${interactionType})`,
      userMessage: variableValue,
      variablesData: { [variableName]: storedValue }
    });

    await db.query(
      `INSERT INTO chatbot_question (sender_id, receiver_id, flow_id, variable_name, variable_value, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, NOW(), NOW())`,
      [sender, receiver, flowId, variableName, storedValue]
    );

    logger.info(`✅ User response successfully saved: ${storedValue} (type: ${interactionType})`);
  } catch (error) {
    logger.error(`❌ Failed to store user response: ${error.message}`);
    throw error;
  }
}


async function sendTextMessage(receiver, message, apiUrl, accessToken, sender, flowId, sendername) {
  const startTime = Date.now();
  
  try {
    if (!message || message.trim() === "") {
      throw new Error("Cannot send empty message");
    }

    if (!apiUrl || !accessToken) {
      throw new Error("API URL or Access Token is missing");
    }

    const finalMessage = await replaceVariables(message, sender, receiver, flowId, {}, sendername);
    
    const payload = {
      messaging_product: "whatsapp",
      to: receiver,
      type: "text",
      text: { body: finalMessage },
    };

    const headers = getHeaders(apiUrl, accessToken);
    const response = await axios.post(apiUrl, payload, { headers });

    if (response.status === 200 && response.data.messages && response.data.messages.length > 0) {
      logger.info("✅ Text message sent successfully.");
      return response.data;
    } else {
      throw new Error("Invalid API response");
    }
  } catch (error) {
    logger.error(`❌ Failed to send text message to ${receiver}:`, error.response?.data || error.message);
    throw error;
  }
}

async function sendInteractiveMessage(apiUrl, payload, accessToken) {
  try {
    const headers = getHeaders(apiUrl, accessToken);
    const response = await axios.post(apiUrl, payload, { headers });
    return response.data;
  } catch (error) {
    logger.error("❌ Failed to send interactive message:", error.response?.data || error.message);
    throw error;
  }
}

// ✅ Additional utility functions remain the same...
async function sendMediaMessage(receiver, caption, mediaType, mediaFile, apiUrl, accessToken, fileName) {
  // Implementation remains the same...
}

function getHeaders(url, apiKey) {
  if (url.startsWith("https://partnersv1.pinbot.ai")) {
    return {
      apikey: apiKey,
      "Content-Type": "application/json",
    };
  } else {
    return {
      Authorization: apiKey,
      "Content-Type": "application/json",
    };
  }
}

function getMediaTypeFromUrl(url) {
  const extension = url.split(".").pop().toLowerCase();
  if (["jpg", "jpeg", "png", "gif"].includes(extension)) {
    return "image";
  } else if (["mp4", "mov", "avi"].includes(extension)) {
    return "video";
  } else if (["pdf", "doc", "docx", "txt"].includes(extension)) {
    return "document";
  } else {
    throw new Error(`Unsupported media type for file: ${url}`);
  }
}

async function replaceVariables(text, sender, receiver, flowId, extra = {}, sendername = null, username = null) {
  if (!text || typeof text !== "string") return text;

  const userVariables = await fetchUserVariables(sender, receiver, flowId);
  const variables = {
    ...userVariables,
    ...extra,
    name: sendername || extra.name || userVariables.name
  };

  const result = text.replace(/@\w+/g, (match) => {
    const key = match.replace("@", "");
    return variables[key] || match;
  });

  return result;
}


async function fetchUserVariables(sender, receiver, flowId) {
  try {
    const [rows] = await db.query(
      `SELECT variable_name, variable_value FROM chatbot_question 
       WHERE sender_id = ? AND receiver_id = ? AND flow_id = ?`,
      [sender, receiver, flowId]
    );

    const variables = {};
    for (const row of rows) {
      variables[row.variable_name] = row.variable_value;
    }
    return variables;
  } catch (error) {
    logger.error(`❌ Failed to fetch user variables: ${error.message}`);
    return {};
  }
}

/** ========================================================================
 * ✅ REMAINING UTILITY FUNCTIONS FOR COMPLETE IMPLEMENTATION
 * ======================================================================== */

/**
 * ✅ Process CTA URL Node
 */
async function processCtaUrlNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  const startTime = Date.now();
  
  try {
    const headerText = currentNode.data.header
      ? await replaceVariables(currentNode.data.header, sender, receiver, flowId, {}, sendername)
      : undefined;
    
    const bodyText = await replaceVariables(currentNode.data.bodyText, sender, receiver, flowId, {}, sendername);
    const footerText = currentNode.data.footer
      ? await replaceVariables(currentNode.data.footer, sender, receiver, flowId, {}, sendername)
      : undefined;
    
    const buttonText = await replaceVariables(currentNode.data.buttonText, sender, receiver, flowId, {}, sendername);
    const url = await replaceVariables(currentNode.data.url, sender, receiver, flowId, {}, sendername);
    
    const ctaPayload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to: receiver,
      type: "interactive",
      interactive: {
        type: "cta_url",
        header: headerText ? { type: "text", text: headerText } : undefined,
        body: { text: bodyText },
        footer: footerText ? { text: footerText } : undefined,
        action: {
          name: "cta_url",
          parameters: {
            display_text: buttonText,
            url: url,
          },
        },
      },
    };

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'button_sent',
      logLevel: 'info',
      message: 'Sending CTA URL message',
      botResponse: bodyText,
      requestPayload: ctaPayload,
      apiEndpoint: apiUrl,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { ctaUrl: url, buttonText }
    });
    
    const ctaRes = await sendInteractiveMessage(apiUrl, ctaPayload, accessToken);
    saveChatMessage({
      username, senderId: sender, receiverId: receiver, senderName: sendername,
      type: 'button',
      text: JSON.stringify([{ text: buttonText, url }]),
      whtsRefId: ctaRes?.messages?.[0]?.id
    });

    const nextEdge = edges.find((edge) => edge.source === currentNode.id);
    if (nextEdge) {
      await processChatbotFlow(
        { flowId, lastNodeId: nextEdge.target, sourceHandles: edges },
        "",
        sender,
        receiver,
        apiUrl,
        accessToken,
        sendername,
        username
      );
    }
  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'button_error',
      logLevel: 'error',
      message: 'Failed to send CTA URL message',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

/**
 * ✅ Process Condition Node
 */
async function processConditionNode(currentNode, receiver, apiUrl, accessToken, flowId, sender, edges, sendername, username) {
  const startTime = Date.now();
  
  try {
    const conditions = JSON.parse(currentNode.data.conditions || "[]");
    const logicOperator = currentNode.data.logicOperator || "AND";
    
    const userVariables = await fetchUserVariables(sender, receiver, flowId);
    
    let conditionMet = false;
    const nextEdge = edges.find((edge) => edge.source === currentNode.id);
    
    if (nextEdge) {
      const [stepsRows] = await db.query("SELECT nodes FROM chatbot_steps WHERE flow_id = ?", [flowId]);
      
      if (stepsRows.length > 0) {
        const nodes = JSON.parse(stepsRows[0].nodes || "[]");
        const nextNode = nodes.find((node) => node.id === nextEdge.target);
        
        if (nextNode && nextNode.type === "condition") {
          conditionMet = await evaluateConditions(conditions, logicOperator, userVariables, "", sender, flowId);
        } else {
          conditionMet = true;
        }
      } else {
        conditionMet = true;
      }
    }

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'condition_evaluated',
      logLevel: 'info',
      message: `Condition ${conditionMet ? 'met' : 'not met'}`,
      variablesData: userVariables,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { 
        conditions, 
        logicOperator, 
        conditionMet,
        nextNodeId: nextEdge ? nextEdge.target : null
      }
    });
    
    if (nextEdge) {
      await processChatbotFlow(
        { flowId, lastNodeId: nextEdge.target, sourceHandles: edges },
        "",
        sender,
        receiver,
        apiUrl,
        accessToken,
        sendername,
        username
      );
    }
  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'system_error',
      logLevel: 'error',
      message: 'Failed to process condition node',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

/**
 * ✅ Process Location Request Node
 */
async function processLocationRequestNode(currentNode, receiver, apiUrl, accessToken, sender, flowId, sendername, username) {
  const startTime = Date.now();
  
  try {
    const { bodyText } = currentNode.data;
    const finalBodyText = await replaceVariables(bodyText, sender, receiver, flowId, {}, sendername);

    const requestLocationPayload = {
      messaging_product: "whatsapp",
      recipient_type: "individual",
      to: receiver,
      type: "interactive",
      interactive: {
        type: "location_request_message",
        body: { text: finalBodyText },
        action: {
          name: "send_location",  
        },
      },
    };

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'location_sent',
      logLevel: 'info',
      message: 'Sending location request',
      botResponse: finalBodyText,
      requestPayload: requestLocationPayload,
      apiEndpoint: apiUrl,
      processingTime: (Date.now() - startTime) / 1000
    });

    const headers = getHeaders(apiUrl, accessToken);
    const response = await axios.post(apiUrl, requestLocationPayload, { headers });
    saveChatMessage({
      username, senderId: sender, receiverId: receiver, senderName: sendername,
      type: 'location',
      text: finalBodyText,
      whtsRefId: response?.data?.messages?.[0]?.id
    });

    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'location_sent',
      logLevel: 'info',
      message: 'Location request sent successfully',
      responsePayload: response.data,
      processingTime: (Date.now() - startTime) / 1000
    });

    return response.data;
  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: currentNode.id,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'location_error',
      logLevel: 'error',
      message: 'Failed to send location request',
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

/**
 * ✅ Process Next Node (for question responses)
 */
async function processNextNode(nextNode, sender, receiver, flowId, message, apiUrl, accessToken, sendername, username) {
  const startTime = Date.now();
  
  try {
    const [stepsRows] = await db.query("SELECT nodes FROM chatbot_steps WHERE flow_id = ?", [flowId]);

    if (!stepsRows.length) {
      throw new Error(`❌ No steps found for Flow ID: ${flowId}`);
    }

    const nodes = JSON.parse(stepsRows[0].nodes || "[]");
    const nextNodeDetails = nodes.find((node) => node.id === nextNode);

    if (!nextNodeDetails) {
      throw new Error(`❌ Next Node ID: ${nextNode} not found in chatbot flow.`);
    }

    await logToPM2({
      username,
      flowId,
      nodeId: nextNode,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'flow_started',
      logLevel: 'info',
      message: `Processing next node after user response`,
      userMessage: message,
      processingTime: (Date.now() - startTime) / 1000,
      metadata: { nextNodeType: nextNodeDetails.type }
    });

    await processNode(
      nextNodeDetails,
      receiver,
      apiUrl,
      accessToken,
      flowId,
      sender,
      [],
      sendername,
      username
    );
  } catch (error) {
    await logToPM2({
      username,
      flowId,
      nodeId: nextNode,
      senderId: sender,
      receiverId: receiver,
      senderName: sendername,
      logType: 'system_error',
      logLevel: 'error',
      message: `Failed to process next node: ${nextNode}`,
      userMessage: message,
      errorDetails: error.message,
      processingTime: (Date.now() - startTime) / 1000
    });
    throw error;
  }
}

/**
 * ✅ Update Session Source
 */
async function updateSessionSource(sender, receiver, newSource) {
  try {
    await db.query(
      `UPDATE chatbot_session SET source = ?, updated_at = NOW() WHERE sender_id = ? AND receiver_id = ?`,
      [newSource, sender, receiver]
    );
    
    await logToPM2({
      username: 'system',
      nodeId: newSource,
      senderId: sender,
      receiverId: receiver,
      logType: 'session_updated',
      logLevel: 'info',
      message: `Session source updated to: ${newSource}`,
      sessionData: { newSource }
    });
  } catch (error) {
    logger.error(`❌ Failed to update session source: ${error.message}`);
    throw error;
  }
}

/**
 * ✅ Build Button Payload
 */
async function buildButtonPayload(receiver, header, bodyText, footer, buttons, sender, receiverId, flowId, sendername) {
  const parsedButtons = JSON.parse(buttons);

  for (let button of parsedButtons) {
    button.text = await replaceVariables(button.text, sender, receiverId, flowId, {}, sendername);
  }

  const finalHeader = header ? await replaceVariables(header, sender, receiverId, flowId, {}, sendername) : undefined;
  const finalBody = await replaceVariables(bodyText, sender, receiverId, flowId, {}, sendername);
  const finalFooter = footer ? await replaceVariables(footer, sender, receiverId, flowId, {}, sendername) : undefined;

  return {
    messaging_product: "whatsapp",
    to: receiver,
    type: "interactive",
    interactive: {
      type: "button",
      header: finalHeader ? { type: "text", text: finalHeader } : undefined,
      body: { text: finalBody },
      footer: finalFooter ? { text: finalFooter } : undefined,
      action: {
        buttons: parsedButtons.map((button) => ({
          type: "reply",
          reply: {
            id: button.id,
            title: button.text,
          },
        })),
      },
    },
  };
}

/**
 * ✅ Send Media Message
 */
async function sendMediaMessage(receiver, caption, mediaType, mediaFile, apiUrl, accessToken, fileName) {
  if (!mediaType || !mediaFile) {
    throw new Error("Invalid media type or file.");
  }

  const mediaPayload = {
    link: mediaFile,
    caption: caption || "",
  };

  if (mediaType.toLowerCase() === "document" && fileName) {
    mediaPayload.filename = fileName;
  }

  const payload = {
    messaging_product: "whatsapp",
    to: receiver,
    type: mediaType.toLowerCase(),
    [mediaType.toLowerCase()]: mediaPayload,
  };

  const headers = getHeaders(apiUrl, accessToken);

  try {
    const response = await axios.post(apiUrl, payload, { headers });
    logger.info(`${mediaType} message sent successfully:`, response.data);
    return response.data;
  } catch (error) {
    logger.error(`Failed to send ${mediaType} message:`, error.response?.data || error.message);
    throw error;
  }
}

/**
 * ✅ Get Button Text By ID
 */
async function getButtonTextById(buttonId, flowId) {
  try {
    const [rows] = await db.query(
      `SELECT nodes FROM chatbot_steps WHERE flow_id = ?`,
      [flowId]
    );

    if (rows.length > 0) {
      const nodes = JSON.parse(rows[0].nodes || "[]");

      for (const node of nodes) {
        if (node.data && node.data.answerOptions) {
          const answerOptions = JSON.parse(node.data.answerOptions || "[]");
          const button = answerOptions.find((btn) => btn.id === buttonId);
          if (button) return button.text;
        }
      }
    }
    return null;
  } catch (error) {
    logger.error(`❌ Failed to fetch button text: ${error.message}`);
    return null;
  }
}

/**
 * ✅ Get List Item Text By ID
 */
async function getListTextById(listItemId, flowId) {
  try {
    const [rows] = await db.query(
      `SELECT nodes FROM chatbot_steps WHERE flow_id = ?`,
      [flowId]
    );

    if (rows.length > 0) {
      const nodes = JSON.parse(rows[0].nodes || "[]");

      for (const node of nodes) {
        if (node.type === 'list' && node.data && node.data.sections) {
          const sections = typeof node.data.sections === 'string'
            ? JSON.parse(node.data.sections)
            : node.data.sections;

          for (const section of sections) {
            if (section.rows && Array.isArray(section.rows)) {
              const listItem = section.rows.find((item) => item.id === listItemId);
              if (listItem) return listItem.text;
            }
          }
        }
      }
    }
    return null;
  } catch (error) {
    logger.error(`❌ Failed to fetch list item text: ${error.message}`);
    return null;
  }
}

/**
 * ✅ Evaluate Conditions
 */
async function evaluateConditions(conditions, logicOperator, variables, message, sender, flowId) {
  if (!Array.isArray(conditions) || conditions.length === 0) {
    return false;
  }

  const results = await Promise.all(
    conditions.map(async (condition) => {
      const { variable, operator, value } = condition;

      let variableValue;
      if (variable.startsWith("@")) {
        const variableName = variable.slice(1);
        variableValue = await fetchVariableFromChatbotQuestion(sender, flowId, variableName);
        if (!variableValue) {
          variableValue = "";
        }
      } else {
        variableValue = message || "";
      }

      switch (operator) {
        case "Equal to":
          return variableValue === value;
        case "Not Equal To":
          return variableValue !== value;
        case "Contains":
          return variableValue.includes(value);
        case "Does Not Contain":
          return !variableValue.includes(value);
        case "Starts With":
          return variableValue.startsWith(value);
        case "Does Not Start With":
          return !variableValue.startsWith(value);
        case "Greater than":
          return parseFloat(variableValue) > parseFloat(value);
        case "Less than":
          return parseFloat(variableValue) < parseFloat(value);
        default:
          return false;
      }
    })
  );

  if (logicOperator === "AND") {
    return results.every((result) => result);
  } else if (logicOperator === "OR") {
    return results.some((result) => result);
  } else {
    return false;
  }
}

/**
 * ✅ Fetch Variable From Chatbot Question
 */
async function fetchVariableFromChatbotQuestion(sender, flowId, variableName) {
  try {
    const [rows] = await db.query(
      `SELECT variable_value FROM chatbot_question WHERE sender_id = ? AND flow_id = ? AND variable_name = ?`,
      [sender, flowId, variableName]
    );

    if (rows.length > 0) {
      return rows[0].variable_value;
    }
    return null;
  } catch (error) {
    logger.error(`❌ Failed to fetch variable "${variableName}": ${error.message}`);
    return null;
  }
}

/**
 * ✅ Get Filename From URL
 */
function getFilenameFromUrl(url) {
  try {
    const pathname = new URL(url).pathname;
    const segments = pathname.split("/");
    const filename = segments.pop();
    return decodeURIComponent(filename);
  } catch (err) {
    logger.error("❌ Invalid URL:", err.message);
    return null;
  }
}

/**
 * ✅ Get Latest Variable Value
 */
async function getLatestVariableValue(sender, receiver, variableName) {
  try {
    const [rows] = await db.query(
      `SELECT variable_value FROM chatbot_question 
       WHERE sender_id = ? AND receiver_id = ? AND variable_name = ? 
       ORDER BY created_at DESC LIMIT 1`,
      [sender, receiver, variableName]
    );

    if (rows.length > 0) {
      return rows[0].variable_value;
    } else {
      return null;
    }
  } catch (error) {
    logger.error(`❌ Error fetching variable value for ${variableName}:`, error.message);
    return null;
  }
}

/**
 * ✅ Log Template Case (Legacy function for compatibility)
 */
async function logTemplateCase({ username, templatePayload, meta, response, error = null }) {
  const query = `
    INSERT INTO chatbot_template_logs 
    (username, payload, meta, response, error) 
    VALUES (?, ?, ?, ?, ?)
  `;
  
  const values = [
    username,
    JSON.stringify(templatePayload),
    JSON.stringify(meta),
    response ? JSON.stringify(response) : null,
    error
  ];

  try {
    await db.execute(query, values);
    logger.info('✅ Template log saved.');
  } catch (err) {
    logger.error('❌ Failed to insert chatbot_template_logs:', err.message);
  }
}

// ✅ Export the status check function
exports.getChatbotStatus = (req, res) => {
  logger.info("🟢 Chatbot server is running.");
  res.send("Chatbot server is running!");
};

// ✅ Additional helper functions and legacy functions remain the same...
async function insertLog(flowId, username, sender, receiver, errorType, errorMessage) {
  try {
    const [result] = await db.query(
      `INSERT INTO chatbot_logs
       (flow_id, username, sender_id, receiver_id, error_type, error_message, created_at)
       VALUES (?, ?, ?, ?, ?, ?, NOW())`,
      [flowId, username, sender, receiver, errorType, errorMessage]
    );
    logger.info(`✅ Log successfully inserted into 'chatbot_logs' for Flow ID: ${flowId}`);
  } catch (logError) {
    logger.error("❌ Failed to insert log into 'chatbot_logs' table:", logError.message);
  }
}

