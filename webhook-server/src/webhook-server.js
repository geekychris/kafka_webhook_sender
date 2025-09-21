const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const path = require('path');

const app = express();
const port = process.env.SERVER_PORT || 8089;

// Middleware
app.use(bodyParser.json({ limit: '10mb' }));
app.use(bodyParser.text());

// Store for received webhooks (in-memory for testing)
const receivedWebhooks = [];
const webhookStats = {
    totalReceived: 0,
    successCount: 0,
    failureCount: 0,
    lastReceived: null
};

// Create logs directory
const logsDir = path.join(__dirname, '..', 'logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

// Utility function to log webhooks to file
function logWebhook(webhook) {
    const logEntry = {
        timestamp: new Date().toISOString(),
        ...webhook
    };
    
    const logFile = path.join(logsDir, `webhook-${new Date().toISOString().split('T')[0]}.log`);
    fs.appendFileSync(logFile, JSON.stringify(logEntry) + '\n');
}

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        stats: webhookStats
    });
});

// Main webhook endpoint - accepts CloudEvents
app.post('/webhook', (req, res) => {
    const webhook = {
        id: Date.now() + Math.random(),
        timestamp: new Date().toISOString(),
        headers: req.headers,
        body: req.body,
        method: req.method,
        url: req.url
    };

    console.log(`📨 Received webhook at ${webhook.timestamp}`);
    console.log('Headers:', JSON.stringify(req.headers, null, 2));
    
    // Handle both single events and batch events
    let events = [];
    if (Array.isArray(req.body)) {
        events = req.body;
        console.log(`📦 Batch of ${events.length} events`);
    } else if (req.body && typeof req.body === 'object') {
        events = [req.body];
        console.log(`📋 Single event`);
    } else {
        console.log(`📄 Raw payload:`, req.body);
    }

    // Log event details
    events.forEach((event, index) => {
        if (event.specversion) {
            console.log(`  Event ${index + 1}:`);
            console.log(`    ID: ${event.id}`);
            console.log(`    Type: ${event.type}`);
            console.log(`    Source: ${event.source}`);
            console.log(`    To: ${event.to || event.subject || 'unknown'}`);
            if (event.data) {
                console.log(`    Data:`, JSON.stringify(event.data, null, 6));
            }
        }
    });

    // Store webhook
    receivedWebhooks.push(webhook);
    
    // Keep only last 1000 webhooks in memory
    if (receivedWebhooks.length > 1000) {
        receivedWebhooks.shift();
    }

    // Update stats
    webhookStats.totalReceived++;
    webhookStats.lastReceived = webhook.timestamp;

    // Log to file
    logWebhook(webhook);

    // Simulate different responses based on headers or content
    const simulateFailure = req.headers['x-simulate-failure'] === 'true';
    const simulateDelay = parseInt(req.headers['x-simulate-delay']) || 0;

    if (simulateDelay > 0) {
        console.log(`⏳ Simulating delay of ${simulateDelay}ms`);
        setTimeout(() => respondToWebhook(), simulateDelay);
    } else {
        respondToWebhook();
    }

    function respondToWebhook() {
        if (simulateFailure) {
            console.log(`❌ Simulating failure response`);
            webhookStats.failureCount++;
            res.status(500).json({
                error: 'Simulated webhook failure',
                timestamp: new Date().toISOString()
            });
        } else {
            console.log(`✅ Webhook processed successfully`);
            webhookStats.successCount++;
            res.status(200).json({
                status: 'success',
                message: 'Webhook received and processed',
                timestamp: new Date().toISOString(),
                eventCount: events.length,
                webhookId: webhook.id
            });
        }
    }
});

// Endpoint to get received webhooks
app.get('/webhooks', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    const offset = parseInt(req.query.offset) || 0;
    
    const paginatedWebhooks = receivedWebhooks
        .slice()
        .reverse()
        .slice(offset, offset + limit);

    res.json({
        webhooks: paginatedWebhooks,
        total: receivedWebhooks.length,
        limit,
        offset,
        stats: webhookStats
    });
});

// Endpoint to get webhook statistics
app.get('/stats', (req, res) => {
    res.json({
        ...webhookStats,
        inMemoryWebhooks: receivedWebhooks.length,
        uptime: process.uptime()
    });
});

// Endpoint to clear stored webhooks
app.delete('/webhooks', (req, res) => {
    const count = receivedWebhooks.length;
    receivedWebhooks.length = 0;
    
    // Reset stats
    webhookStats.totalReceived = 0;
    webhookStats.successCount = 0;
    webhookStats.failureCount = 0;
    webhookStats.lastReceived = null;
    
    res.json({
        message: `Cleared ${count} stored webhooks`,
        timestamp: new Date().toISOString()
    });
});

// Custom webhook endpoints for testing different scenarios
app.post('/webhook/slow', (req, res) => {
    console.log('🐌 Slow webhook endpoint called');
    setTimeout(() => {
        res.json({ status: 'success', message: 'Slow response', delay: '2s' });
    }, 2000);
});

app.post('/webhook/fail', (req, res) => {
    console.log('💥 Failing webhook endpoint called');
    webhookStats.failureCount++;
    res.status(500).json({ error: 'This endpoint always fails' });
});

app.post('/webhook/auth', (req, res) => {
    const authHeader = req.headers.authorization;
    console.log('🔐 Auth webhook endpoint called');
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Missing or invalid authorization header' });
    }
    
    console.log('✅ Authorization successful');
    res.json({ 
        status: 'success', 
        message: 'Authenticated webhook received',
        token: authHeader.substring(0, 20) + '...'
    });
});

// Error handling middleware
app.use((error, req, res, next) => {
    console.error('❌ Error:', error);
    res.status(500).json({
        error: 'Internal server error',
        timestamp: new Date().toISOString()
    });
});

// Start server
app.listen(port, '0.0.0.0', () => {
    console.log(`🚀 Test webhook server running on port ${port}`);
    console.log(`📋 Available endpoints:`);
    console.log(`   GET  /health          - Health check`);
    console.log(`   POST /webhook         - Main webhook endpoint`);
    console.log(`   GET  /webhooks        - List received webhooks`);
    console.log(`   GET  /stats           - Webhook statistics`);
    console.log(`   DELETE /webhooks      - Clear stored webhooks`);
    console.log(`   POST /webhook/slow    - Slow response (2s delay)`);
    console.log(`   POST /webhook/fail    - Always fails with 500`);
    console.log(`   POST /webhook/auth    - Requires Bearer token`);
    console.log(`📁 Logs are written to: ${logsDir}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Received SIGTERM, shutting down gracefully');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('🛑 Received SIGINT, shutting down gracefully');
    process.exit(0);
});