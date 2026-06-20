const axios = require("axios");
const db = require("../config/db");
const logger = require("../utils/logger");

class ChatbotService {
  constructor() {
    this.openaiApiKey = process.env.OPENAI_API_KEY;
    this.geminiApiKey = process.env.GEMINI_API_KEY;
    this.openaiApiUrl = "https://api.openai.com/v1/chat/completions";
    this.geminiApiUrl = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent";
    this.llamaApiUrl = "http://localhost:11434/api/generate";

    logger.info(`🔑 API Keys Status: Gemini: ${this.geminiApiKey ? 'SET' : 'NOT SET'}, OpenAI: ${this.openaiApiKey ? 'SET' : 'NOT SET'}`);
  }

  isUnknownColumnError(error) {
    return error?.code === "ER_BAD_FIELD_ERROR" || /Unknown column/i.test(error?.message || "");
  }

  async getSessionAIMemory(sender, receiver) {
    try {
      const [rows] = await db.query(
        `SELECT ai, ai_memory
         FROM chatbot_session
         WHERE sender_id = ? AND receiver_id = ?
         ORDER BY updated_at DESC
         LIMIT 1`,
        [sender, receiver]
      );

      if (!rows.length || Number(rows[0].ai) !== 1 || !rows[0].ai_memory) {
        return [];
      }

      const parsed = JSON.parse(rows[0].ai_memory);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      if (!this.isUnknownColumnError(error)) {
        logger.error(`❌ Failed to read AI session memory: ${error.message}`);
      }
      return [];
    }
  }

  buildMemoryContext(aiMemory) {
    if (!Array.isArray(aiMemory) || aiMemory.length === 0) return "";

    const memoryLines = aiMemory
      .slice(-8)
      .map((item) => `${item.role === "assistant" ? "Assistant" : "User"}: ${item.content}`)
      .join("\n");

    return `Recent conversation context:\n${memoryLines}\n`;
  }

  async updateSessionAIMemory(sender, receiver, aiNodeId, userMessage, assistantMessage) {
    try {
      const existingMemory = await this.getSessionAIMemory(sender, receiver);
      const nextMemory = [
        ...existingMemory,
        { role: "user", content: userMessage, ts: new Date().toISOString() },
        { role: "assistant", content: assistantMessage, ts: new Date().toISOString() }
      ].slice(-12);

      const expiryTime = new Date(Date.now() + 1 * 60 * 60 * 1000);
      await db.query(
        `UPDATE chatbot_session
         SET source = ?, message_type = 'ai', ai = 1, ai_memory = ?, expiry_time = ?, updated_at = NOW()
         WHERE sender_id = ? AND receiver_id = ?`,
        [aiNodeId, JSON.stringify(nextMemory), expiryTime, sender, receiver]
      );
    } catch (error) {
      if (this.isUnknownColumnError(error)) {
        await this.keepUserInAINode(aiNodeId, sender, receiver);
        return;
      }
      logger.error(`❌ Failed to update AI session memory: ${error.message}`);
      throw error;
    }
  }

  /**
   * Process AI Node with Enhanced Intent Recognition and Session Management
   */
  async processAINode(aiNode, userMessage, sender, receiver, flowId, apiUrl, accessToken, sendername, username, nodes, edges) {
    const startTime = Date.now();
    const sessionId = `${sender}_${receiver}_${Date.now()}`;

    // Create detailed AI usage log entry
    await this.createAIUsageLog({
      sessionId,
      username,
      sender,
      receiver,
      question: userMessage,
      flowNodeId: aiNode.id,
      status: 'started',
      startTime: new Date()
    });

    try {
      logger.info(`🤖 AI node processing started for: "${userMessage}"`);

      const smartRoutes = JSON.parse(aiNode.data.smartRoutes || "[]");
      const systemPrompt = aiNode.data.systemPrompt || "You are a helpful assistant.";
      const fallbackMessage = aiNode.data.fallbackMessage || "I'm sorry, I couldn't understand that. Please try again.";
      const aiMemory = await this.getSessionAIMemory(sender, receiver);

      logger.info(`📊 Smart routes configured: ${smartRoutes.length}`);

      // ALWAYS try AI analysis first
      const routeDecision = await this.analyzeUserIntent(userMessage, smartRoutes, systemPrompt, sessionId);

      if (!routeDecision || !routeDecision.matchedRoute) {
        // Update usage log with fallback
        await this.updateAIUsageLog(sessionId, {
          aiResponse: fallbackMessage,
          status: 'fallback',
          error: 'No route matched by AI or keyword analysis',
          endTime: new Date(),
          processingTimeMs: Date.now() - startTime
        });

        await this.sendFallbackMessage(receiver, fallbackMessage, apiUrl, accessToken, sender, flowId, sendername, username);
        await this.updateSessionAIMemory(sender, receiver, aiNode.id, userMessage, fallbackMessage);
        return;
      }

      const matchedRoute = routeDecision.matchedRoute;
      logger.info(`✅ AI Intent Recognition: Route "${matchedRoute.actionType}" selected with confidence: ${routeDecision.confidence}`);
      logger.info(`🧠 AI Reasoning: ${routeDecision.reasoning}`);

      // Update usage log with successful recognition
      await this.updateAIUsageLog(sessionId, {
        matchedRoute: JSON.stringify(matchedRoute),
        confidence: routeDecision.confidence,
        reasoning: routeDecision.reasoning,
        status: 'route_matched'
      });

      switch (matchedRoute.actionType) {
        case 'node':
          await this.executeNodeAction(matchedRoute, sender, receiver, flowId, apiUrl, accessToken, sendername, username, nodes, edges);
          await this.updateAIUsageLog(sessionId, { status: 'node_redirect', endTime: new Date() });
          break;
        case 'chatbot':
          // Pass the node's systemPrompt so the reply stays grounded in the configured role
          {
            const assistantMessage = await this.executeChatbotAction(matchedRoute, userMessage, receiver, apiUrl, accessToken, sender, flowId, sendername, username, sessionId, systemPrompt, aiMemory);
            await this.updateSessionAIMemory(sender, receiver, aiNode.id, userMessage, assistantMessage);
          }
          break;
        case 'api':
          await this.executeApiAction(matchedRoute, userMessage, receiver, apiUrl, accessToken, sender, flowId, sendername, username);
          await this.updateSessionAIMemory(sender, receiver, aiNode.id, userMessage, "API response sent");
          await this.updateAIUsageLog(sessionId, { status: 'api_call_completed', endTime: new Date() });
          break;
        default:
          await this.sendFallbackMessage(receiver, fallbackMessage, apiUrl, accessToken, sender, flowId, sendername, username);
          await this.updateSessionAIMemory(sender, receiver, aiNode.id, userMessage, fallbackMessage);
          await this.updateAIUsageLog(sessionId, { status: 'unknown_action_type', error: `Unknown action type: ${matchedRoute.actionType}` });
      }

      const processingTime = Date.now() - startTime;
      logger.info(`⚡ AI node processing completed in ${processingTime}ms`);

      // Final usage log update
      await this.updateAIUsageLog(sessionId, {
        processingTimeMs: processingTime,
        endTime: new Date()
      });

    } catch (error) {
      logger.error("❌ Error in AI node processing:", error.message);

      // Log error details
      await this.updateAIUsageLog(sessionId, {
        status: 'error',
        error: error.message,
        endTime: new Date(),
        processingTimeMs: Date.now() - startTime
      });

      try {
        await this.keepUserInAINode(aiNode.id, sender, receiver);
      } catch (keepError) {
        logger.error("Failed to keep user in AI node after error:", keepError.message);
      }

      throw error;
    }
  }

  /**
   * Keep user in the current AI node for continued conversation
   */
  async keepUserInAINode(aiNodeId, sender, receiver) {
    try {
      const expiryTime = new Date(Date.now() + 1 * 60 * 60 * 1000); // 1 hour
      await db.query(
        `UPDATE chatbot_session 
         SET source = ?, message_type = 'ai', ai = 1, expiry_time = ?, updated_at = NOW() 
         WHERE sender_id = ? AND receiver_id = ?`,
        [aiNodeId, expiryTime, sender, receiver]
      );
      logger.info(`✅ User session maintained in AI node ${aiNodeId} for continued conversation`);
    } catch (error) {
      if (this.isUnknownColumnError(error)) {
        await db.query(
          `UPDATE chatbot_session 
           SET source = ?, message_type = 'ai', expiry_time = ?, updated_at = NOW() 
           WHERE sender_id = ? AND receiver_id = ?`,
          [aiNodeId, expiryTime, sender, receiver]
        );
        logger.info(`✅ User session maintained in AI node ${aiNodeId} for continued conversation`);
        return;
      }
      logger.error("❌ Failed to keep user in AI node:", error.message);
      throw error;
    }
  }

  /**
   * Enhanced Intent Analysis - AI understands meaning, not just keywords
   */
  async analyzeUserIntent(userMessage, smartRoutes, systemPrompt, sessionId) {
    logger.info(`🔍 Starting AI intent analysis for: "${userMessage}"`);

    // Check if we have ANY AI service configured
    if (!this.geminiApiKey && !this.openaiApiKey) {
      logger.warn("⚠️ No AI API keys configured! Falling back to keyword matching immediately");
      await this.updateAIUsageLog(sessionId, {
        error: 'No AI API keys configured',
        aiProvider: 'none'
      });
      return this.fallbackKeywordMatching(userMessage, smartRoutes);
    }

    try {
      const analysisPrompt = this.buildEnhancedAnalysisPrompt(userMessage, smartRoutes, systemPrompt);

      // Try AI analysis — jsonMode=true so providers return clean JSON
      const aiResponse = await this.callAI(analysisPrompt, sessionId, true);
      const decision = this.parseAIRouteDecision(aiResponse, smartRoutes);

      if (decision) {
        logger.info(`✅ AI Intent Recognition successful: ${decision.reasoning}`);
        await this.updateAIUsageLog(sessionId, {
          aiResponse: aiResponse.substring(0, 500),
          status: 'ai_analysis_success'
        });
        return decision;
      } else {
        logger.warn("⚠️ AI returned invalid decision format");
        await this.updateAIUsageLog(sessionId, {
          aiResponse: aiResponse,
          error: 'Invalid AI decision format',
          status: 'ai_analysis_failed'
        });
      }

    } catch (error) {
      logger.error("❌ AI intent analysis failed:", error.message);
      await this.updateAIUsageLog(sessionId, {
        error: error.message,
        status: 'ai_analysis_error'
      });
    }

    // Fallback to keyword matching
    logger.warn("🔄 AI intent analysis failed, falling back to keyword matching");
    return this.fallbackKeywordMatching(userMessage, smartRoutes);
  }

  /**
   * Build Enhanced Analysis Prompt - Focus on Intent, not Keywords
   */
  buildEnhancedAnalysisPrompt(userMessage, smartRoutes, systemPrompt) {
    const routesDescription = smartRoutes.map((route, index) => {
      let description = `Route ${index} (${route.actionType}):`;

      // Add route purpose based on action type and config
      if (route.actionType === 'chatbot' && route.actionConfig?.aiResponse) {
        description += ` For ${route.actionConfig.aiResponse}`;
      } else if (route.actionType === 'node') {
        description += ` Navigate to specific node/section`;
      } else if (route.actionType === 'api') {
        description += ` API data retrieval`;
      }

      // Use intentPrompt instead of keywords
      description += ` | Intent: ${route.intentPrompt}`;
      return description;
    }).join('\n');

    return `${systemPrompt}

INTENT ANALYSIS TASK:
Analyze the user's message and determine which route best matches their INTENT, not just keyword matching.

User Message: "${userMessage}"

Available Routes:
${routesDescription}

ANALYSIS INSTRUCTIONS:
1. Understand what the user actually WANTS to accomplish
2. Consider the CONTEXT and PURPOSE of each route
3. Keywords are just hints - focus on the user's INTENT
4. Consider semantic similarity and meaning
5. Think about what would be most helpful to the user

Return ONLY this JSON format:
{
  "routeIndex": 0,
  "confidence": 0.85,
  "matchedIntent": "description of matched intent",
  "reasoning": "Why this route matches the user's intent - explain the semantic understanding"
}

If no route matches well (confidence < 0.4), return:
{
  "routeIndex": -1,
  "confidence": 0.3,
  "matchedIntent": "",
  "reasoning": "No route clearly matches the user's intent"
}`;
  }

  /**
   * Enhanced AI Call with Better Error Handling and Logging
   * @param {boolean} jsonMode - true for intent analysis (JSON output), false for free-text replies
   */
  async callAI(prompt, sessionId, jsonMode = false) {
    logger.info(`🚀 Starting AI call (jsonMode=${jsonMode}) with prompt length: ${prompt.length}`);

    try {
      // Try Gemini first
      if (this.geminiApiKey) {
        try {
          logger.info("🔮 Using Gemini...");
          await this.updateAIUsageLog(sessionId, { aiProvider: 'gemini' });
          const result = await this.callGeminiAPI(prompt, jsonMode);
          logger.info("✅ Gemini call successful");
          await this.updateAIUsageLog(sessionId, {
            aiProviderUsed: 'gemini',
            tokensUsed: this.estimateTokens(prompt + result)
          });
          return result;
        } catch (geminiError) {
          logger.error("❌ Gemini API error:", geminiError.message);
          await this.updateAIUsageLog(sessionId, {
            error: `Gemini failed: ${geminiError.message}`,
            aiProvider: 'gemini_failed'
          });
        }
      }

      // Fallback to OpenAI
      if (this.openaiApiKey) {
        try {
          logger.info("🤖 Using OpenAI...");
          await this.updateAIUsageLog(sessionId, { aiProvider: 'openai' });
          const result = await this.callOpenAI(prompt, jsonMode);
          logger.info("✅ OpenAI call successful");
          await this.updateAIUsageLog(sessionId, {
            aiProviderUsed: 'openai',
            tokensUsed: this.estimateTokens(prompt + result)
          });
          return result;
        } catch (openaiError) {
          logger.error("❌ OpenAI API error:", openaiError.message);
          await this.updateAIUsageLog(sessionId, {
            error: `OpenAI failed: ${openaiError.message}`,
            aiProvider: 'openai_failed'
          });
        }
      }

      // Last resort: Local Llama
      try {
        logger.info("🦙 Using Llama...");
        await this.updateAIUsageLog(sessionId, { aiProvider: 'llama' });
        const result = await this.callLlamaAPI(prompt);
        logger.info("✅ Llama call successful");
        await this.updateAIUsageLog(sessionId, {
          aiProviderUsed: 'llama',
          tokensUsed: this.estimateTokens(prompt + result)
        });
        return result;
      } catch (llamaError) {
        logger.error("❌ Llama API error:", llamaError.message);
        await this.updateAIUsageLog(sessionId, {
          error: `All AI services failed. Llama: ${llamaError.message}`,
          aiProvider: 'all_failed'
        });
        throw new Error("All AI services unavailable");
      }

    } catch (error) {
      logger.error("🔥 Critical error in AI call:", error.message);
      await this.updateAIUsageLog(sessionId, {
        error: `Critical error: ${error.message}`,
        status: 'critical_error'
      });
      throw error;
    }
  }

  /**
   * Gemini API Call
   * @param {boolean} jsonMode - request JSON output when true
   */
  async callGeminiAPI(prompt, jsonMode = false) {
    if (!this.geminiApiKey) {
      throw new Error("Gemini API key is not configured");
    }

    const payload = {
      contents: [{
        parts: [{ text: prompt }]
      }],
      generationConfig: {
        temperature: 0.2,
        topK: 20,
        topP: 0.8,
        maxOutputTokens: 500,
        candidateCount: 1,
        thinkingConfig: {
          thinkingBudget: 0
        }
      },
      safetySettings: [
        { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_MEDIUM_AND_ABOVE" },
        { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_MEDIUM_AND_ABOVE" },
        { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_MEDIUM_AND_ABOVE" },
        { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_MEDIUM_AND_ABOVE" }
      ]
    };

    // Ask Gemini for strict JSON only during intent analysis
    if (jsonMode) {
      payload.generationConfig.responseMimeType = "application/json";
    }

    const url = `${this.geminiApiUrl}?key=${this.geminiApiKey}`;

    try {
      const response = await axios.post(url, payload, {
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'IntentBot/1.0'
        },
        timeout: 15000
      });

      if (response.data?.candidates?.[0]?.content?.parts?.[0]?.text) {
        let result = response.data.candidates[0].content.parts[0].text.trim();
        logger.info("✅ Gemini response received");
        return result;
      } else {
        logger.error("❌ Invalid Gemini response");
        throw new Error("Invalid Gemini API response format");
      }
    } catch (error) {
      if (error.response) {
        throw new Error(`Gemini API error: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
      } else {
        throw new Error(`Gemini API error: ${error.message}`);
      }
    }
  }

  /**
   * OpenAI API Call
   * @param {boolean} jsonMode - force JSON object output when true (intent analysis)
   */
  async callOpenAI(prompt, jsonMode = false) {
    if (!this.openaiApiKey) {
      throw new Error("OpenAI API key is not configured");
    }

    const payload = {
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content: "You are an expert at understanding user intent and producing helpful, on-role responses. Focus on semantic meaning, not just keywords. Never mention any company, product, or website that is not part of the role you are given."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      max_tokens: 500,
      temperature: 0.2,
      frequency_penalty: 0,
      presence_penalty: 0
    };

    // Only force JSON for intent analysis; free-text replies must stay plain text
    if (jsonMode) {
      payload.response_format = { type: "json_object" };
    }

    const headers = {
      'Authorization': `Bearer ${this.openaiApiKey}`,
      'Content-Type': 'application/json',
      'User-Agent': 'IntentBot/1.0'
    };

    try {
      const response = await axios.post(this.openaiApiUrl, payload, {
        headers,
        timeout: 20000
      });

      if (response.data?.choices?.[0]) {
        let result = response.data.choices[0].message.content.trim();
        logger.info("✅ OpenAI response received");
        return result;
      } else {
        throw new Error("Invalid OpenAI response format");
      }
    } catch (error) {
      if (error.response?.status === 429) {
        throw new Error("OpenAI quota exceeded");
      } else if (error.response) {
        throw new Error(`OpenAI API error: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
      } else {
        throw new Error(`OpenAI API error: ${error.message}`);
      }
    }
  }

  /**
   * Llama API Call
   */
  async callLlamaAPI(prompt) {
    const payload = {
      model: "llama3.2:3b",
      prompt: prompt,
      stream: false,
      options: {
        temperature: 0.2,
        top_p: 0.8,
        top_k: 20
      }
    };

    try {
      const response = await axios.post(this.llamaApiUrl, payload, {
        headers: { "Content-Type": "application/json" },
        timeout: 30000
      });

      if (response.data && response.data.response) {
        let result = response.data.response.trim();
        logger.info("✅ Llama response received");
        return result;
      } else {
        throw new Error("Invalid Llama response format");
      }
    } catch (error) {
      if (error.code === 'ECONNREFUSED') {
        throw new Error("Local Llama server not running");
      } else {
        throw new Error(`Llama API error: ${error.message}`);
      }
    }
  }

  /**
   * Enhanced AI Route Decision Parser
   */
  parseAIRouteDecision(aiResponse, smartRoutes) {
    try {
      logger.info(`🔍 Parsing AI intent decision: ${aiResponse.substring(0, 200)}...`);

      // Clean response - handle various formats
      let cleanResponse = aiResponse
        .replace(/```json\n?/g, '')
        .replace(/```\n?/g, '')
        .replace(/^[^{]*\{/, '{') // Remove text before JSON
        .replace(/\}[^}]*$/, '}') // Remove text after JSON
        .trim();

      // Try to extract JSON
      const jsonMatch = cleanResponse.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        cleanResponse = jsonMatch[0];
      }

      const decision = JSON.parse(cleanResponse);

      // Validate decision structure
      if (typeof decision.routeIndex !== 'number' || typeof decision.confidence !== 'number') {
        logger.warn("⚠️ Invalid AI decision structure");
        return null;
      }

      // Check confidence threshold (lowered for better coverage)
      if (decision.routeIndex === -1 || decision.confidence < 0.4) {
        logger.info(`ℹ️ AI decision: Low confidence (${decision.confidence}) or no route selected`);
        return null;
      }

      const matchedRoute = smartRoutes[decision.routeIndex];
      if (!matchedRoute) {
        logger.warn(`⚠️ AI selected invalid route index: ${decision.routeIndex}`);
        return null;
      }

      logger.info(`✅ AI Intent Decision: Route ${decision.routeIndex} selected with confidence ${decision.confidence}`);
      logger.info(`🧠 AI Reasoning: ${decision.reasoning}`);

      return {
        matchedRoute,
        confidence: decision.confidence,
        matchedIntent: decision.matchedIntent || "",
        reasoning: decision.reasoning || "AI semantic analysis"
      };
    } catch (error) {
      logger.error("❌ Error parsing AI intent decision:", error.message);
      logger.error("📝 Raw AI response:", aiResponse);
      return null;
    }
  }

  /**
   * Fallback Intent Matching - Only when AI fails
   */
  fallbackKeywordMatching(userMessage, smartRoutes) {
    const userMessageLower = userMessage.toLowerCase();
    let bestMatch = null;
    let bestScore = 0;

    logger.info("🔄 Using fallback intent matching (AI intent analysis failed)");

    for (const route of smartRoutes) {
      // Extract potential keywords from intentPrompt
      const intentPrompt = route.intentPrompt || "";
      const intentLower = intentPrompt.toLowerCase();

      // Simple word matching from intent prompt
      const intentWords = intentLower.split(/[\s,]+/).filter(word => word.length > 2);
      let matchCount = 0;
      const matchedWords = [];

      for (const word of intentWords) {
        if (userMessageLower.includes(word)) {
          matchCount++;
          matchedWords.push(word);
        }
      }

      // Calculate score based on word matches
      const score = intentWords.length > 0 ? matchCount / intentWords.length : 0;
      if (score > bestScore && score >= 0.1) { // Very low threshold as last resort
        bestScore = score;
        bestMatch = {
          matchedRoute: route,
          confidence: score,
          matchedIntent: matchedWords.join(", "),
          reasoning: "Fallback intent matching (AI unavailable)"
        };
      }
    }

    if (bestMatch) {
      logger.info(`✅ Intent fallback match: confidence ${bestMatch.confidence}`);
    } else {
      logger.info("❌ No intent matches found");
    }

    return bestMatch;
  }

  /**
   * Execute Node Action - Navigate to a specific node
   */
  async executeNodeAction(matchedRoute, sender, receiver, flowId, apiUrl, accessToken, sendername, username, nodes, edges) {
    const targetNodeId = matchedRoute.actionConfig.nodeId;
    const targetNode = nodes.find(node => node.id === targetNodeId);
    if (!targetNode) throw new Error(`Target node ${targetNodeId} not found`);

    logger.info(`🚀 Executing node action: Navigating to node ${targetNodeId}`);

    const expiryTime = new Date(Date.now() + 1 * 60 * 60 * 1000);
    try {
      await db.query(
        `UPDATE chatbot_session 
         SET source = ?, message_type = ?, ai = 0, ai_memory = NULL, expiry_time = ?, updated_at = NOW() 
         WHERE sender_id = ? AND receiver_id = ?`,
        [targetNodeId, 'normal', expiryTime, sender, receiver]
      );
    } catch (error) {
      if (this.isUnknownColumnError(error)) {
        await db.query(
          `UPDATE chatbot_session 
           SET source = ?, message_type = ?, expiry_time = ?, updated_at = NOW() 
           WHERE sender_id = ? AND receiver_id = ?`,
          [targetNodeId, 'normal', expiryTime, sender, receiver]
        );
      } else {
        throw error;
      }
    }

    logger.info(`✅ Session updated: User will navigate to node ${targetNodeId} on next message`);
  }

  /**
   * Execute Chatbot Action - Respond and stay in AI node
   * Now grounded in the node's systemPrompt so replies stay on-role.
   */
  async executeChatbotAction(matchedRoute, userMessage, receiver, apiUrl, accessToken, sender, flowId, sendername, username, sessionId, systemPrompt = "", aiMemory = []) {
    const aiResponseTemplate = matchedRoute.actionConfig.aiResponse || "";
    const memoryContext = this.buildMemoryContext(aiMemory);

    // Build a prompt that keeps the model strictly inside the configured role.
    const responsePrompt = `${systemPrompt}

---
A user message matched one of your routes. Use the "reply direction" only as a hint for tone/direction, but answer the user's actual question fully and accurately based on YOUR ROLE above.

${memoryContext}

Reply direction hint: ${aiResponseTemplate}
User Name: ${sendername || 'Customer'}
User Question: "${userMessage}"

Instructions:
- Stay strictly within the role defined above. NEVER mention any company, product, or website that is not part of your role.
- Keep the reply under 1000 characters (WhatsApp friendly).
- Be friendly and professional; address the user by name when it feels natural.
- Respond in the user's language.
- If something is outside your knowledge, say so politely and suggest contacting the office. Never invent details.

Response:`;

    let finalResponse;
    try {
      // jsonMode=false -> plain text reply
      finalResponse = await this.callAI(responsePrompt, sessionId, false);

      // Clean up response
      finalResponse = finalResponse
        .replace(/^(Response:|Here('|')s|Here is)/i, '')
        .replace(/^["']|["']$/g, '') // Remove wrapping quotes
        .trim();

      if (finalResponse.length > 1000) {
        finalResponse = finalResponse.substring(0, 997) + "...";
      }

      logger.info(`✅ Generated AI response: ${finalResponse}`);

      await this.updateAIUsageLog(sessionId, {
        aiResponse: finalResponse,
        status: 'chatbot_response_generated'
      });

    } catch (aiError) {
      logger.warn("⚠️ AI response generation failed:", aiError.message);
      finalResponse = this.generateContextualFallback(aiResponseTemplate, userMessage, sendername);

      await this.updateAIUsageLog(sessionId, {
        aiResponse: finalResponse,
        error: `AI response failed: ${aiError.message}`,
        status: 'fallback_response_used'
      });
    }

    // Send response but DON'T change session source - stay in AI node
    await this.sendTextMessage(receiver, finalResponse, apiUrl, accessToken, sender, flowId, sendername);

    logger.info("📤 Chatbot response sent - user remains in AI node for continued conversation");

    return finalResponse;
  }

  /**
   * Generate fallback response - uses the route's own configured reply.
   * No hardcoded brand/company text, so nothing can leak across flows.
   */
  generateContextualFallback(template, userMessage, sendername) {
    const userName = sendername || 'there';

    // The route's own configured reply is the safest, on-brand fallback.
    if (template && template.trim()) {
      return template.trim();
    }

    // Generic, brand-neutral last resort.
    return `Hi ${userName}, I didn't quite catch that. Could you please rephrase your question?`;
  }

  async executeApiAction(matchedRoute, userMessage, receiver, apiUrl, accessToken, sender, flowId, sendername, username) {
    try {
      logger.info("🔗 Executing API action");
      const apiConfig = matchedRoute.actionConfig;
      const apiParams = await this.extractApiParameters(apiConfig, userMessage, sender, receiver, flowId);
      const apiResponse = await this.makeApiCall(apiConfig, apiParams);

      // Use responseTemplate if available, otherwise format the response
      let formattedResponse;
      if (apiConfig.responseTemplate) {
        formattedResponse = apiConfig.responseTemplate;
      } else {
        formattedResponse = await this.formatApiResponse(apiResponse, apiConfig.responseFormat);
      }

      await this.sendTextMessage(receiver, formattedResponse, apiUrl, accessToken, sender, flowId, sendername);
      logger.info("✅ API response sent - user remains in AI node for continued conversation");
    } catch (error) {
      logger.error("❌ API action execution error:", error.message);
      await this.sendTextMessage(receiver, "I'm sorry, I couldn't retrieve that information right now. Please try again in a moment.", apiUrl, accessToken, sender, flowId, sendername);
    }
  }

  async extractApiParameters(apiConfig, userMessage, sender, receiver, flowId) {
    // Parse the requestBody template and replace placeholders
    let requestBody = apiConfig.requestBody || "{}";

    // Replace placeholders in requestBody
    requestBody = requestBody
      .replace(/\{userMessage\}/g, userMessage)
      .replace(/\{sender\}/g, sender)
      .replace(/\{receiver\}/g, receiver)
      .replace(/\{flowId\}/g, flowId)
      .replace(/\{timestamp\}/g, new Date().toISOString());

    try {
      return JSON.parse(requestBody);
    } catch (error) {
      logger.warn("Failed to parse requestBody, using default parameters");
      return { userMessage, sender, receiver, flowId, timestamp: new Date().toISOString() };
    }
  }

  async makeApiCall(apiConfig, params) {
    const { method, apiUrl, headers = {}, body = {} } = apiConfig;
    const config = {
      method: method.toLowerCase(),
      url: apiUrl,
      headers: { 'Content-Type': 'application/json', ...headers },
      timeout: 10000
    };

    if (['post', 'put', 'patch'].includes(method.toLowerCase())) {
      config.data = { ...body, ...params };
    } else {
      config.params = params;
    }

    const response = await axios(config);
    return response.data;
  }

  async formatApiResponse(apiResponse, format = null) {
    if (format) {
      return format.replace(/\{(\w+)\}/g, (match, key) => apiResponse[key] || match);
    }
    return JSON.stringify(apiResponse, null, 2);
  }

  async sendFallbackMessage(receiver, fallbackMessage, apiUrl, accessToken, sender, flowId, sendername, username) {
    logger.info("📤 Sending fallback message - no route matched user intent");
    await this.sendTextMessage(receiver, fallbackMessage, apiUrl, accessToken, sender, flowId, sendername);
  }

  async sendTextMessage(receiver, message, apiUrl, accessToken, sender, flowId, sendername) {
    if (!message || message.trim() === "") throw new Error("Cannot send empty message");
    if (!apiUrl || !accessToken) throw new Error("API URL or Access Token is missing");

    const payload = {
      messaging_product: "whatsapp",
      to: receiver,
      type: "text",
      text: { body: message },
    };

    const headers = this.getHeaders(apiUrl, accessToken);
    const response = await axios.post(apiUrl, payload, { headers, timeout: 10000 });

    if (!(response.status === 200 && response.data?.messages?.length > 0)) {
      throw new Error("Invalid API response");
    }

    logger.info(`📨 Message sent successfully: "${message.substring(0, 50)}..."`);
    return response.data;
  }

  getHeaders(url, apiKey) {
    if (url.startsWith("https://partnersv1.pinbot.ai")) {
      return { apikey: apiKey, "Content-Type": "application/json" };
    }
    return { Authorization: apiKey, "Content-Type": "application/json" };
  }

  async updateSessionSource(sender, receiver, newSource) {
    await db.query(
      `UPDATE chatbot_session SET source = ?, updated_at = NOW() WHERE sender_id = ? AND receiver_id = ?`,
      [newSource, sender, receiver]
    );
  }

  /**
   * Create AI Usage Log Entry
   */
  async createAIUsageLog(logData) {
    try {
      const query = `INSERT INTO chatbot_ai_usage (
        session_id, username, sender, receiver, question, ai_response, 
        ai_provider, ai_provider_used, tokens_used, matched_route, 
        confidence, reasoning, error, flow_node_id, status, 
        processing_time_ms, start_time, end_time, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`;

      await db.query(query, [
        logData.sessionId,
        logData.username || null,
        logData.sender,
        logData.receiver,
        logData.question,
        logData.aiResponse || null,
        logData.aiProvider || null,
        logData.aiProviderUsed || null,
        logData.tokensUsed || null,
        logData.matchedRoute || null,
        logData.confidence || null,
        logData.reasoning || null,
        logData.error || null,
        logData.flowNodeId,
        logData.status,
        logData.processingTimeMs || null,
        logData.startTime,
        logData.endTime || null
      ]);

      logger.info(`📊 AI usage log created: ${logData.sessionId}`);
    } catch (error) {
      logger.error("❌ Failed to create AI usage log:", error.message);
    }
  }

  /**
   * Update AI Usage Log Entry
   */
  async updateAIUsageLog(sessionId, updateData) {
    try {
      const setParts = [];
      const values = [];

      Object.entries(updateData).forEach(([key, value]) => {
        const columnMap = {
          aiResponse: 'ai_response',
          aiProvider: 'ai_provider',
          aiProviderUsed: 'ai_provider_used',
          tokensUsed: 'tokens_used',
          matchedRoute: 'matched_route',
          processingTimeMs: 'processing_time_ms',
          startTime: 'start_time',
          endTime: 'end_time'
        };

        const column = columnMap[key] || key;
        setParts.push(`${column} = ?`);
        values.push(value);
      });

      if (setParts.length === 0) return;

      setParts.push('updated_at = NOW()');
      values.push(sessionId);

      const query = `UPDATE chatbot_ai_usage SET ${setParts.join(', ')} WHERE session_id = ?`;
      await db.query(query, values);

    } catch (error) {
      logger.error("❌ Failed to update AI usage log:", error.message);
    }
  }

  /**
   * Estimate token usage for cost tracking
   */
  estimateTokens(text) {
    // Rough estimation: 1 token ≈ 4 characters
    return Math.ceil(text.length / 4);
  }
}

module.exports = new ChatbotService();
