const express = require("express");
const fetch = require("node-fetch");
const cron = require("node-cron");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json());

// ================= CONFIG =================
const SHOP = "mello-distribution-test.myshopify.com";
const CLIENT_ID = "16268ecdaf07c5ebef41d27e9f78c7db";
const CLIENT_SECRET = "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";
const SCOPES = "read_products,write_products,read_price_rules,write_price_rules,read_webhooks,write_webhooks";

const REDIRECT_URI = process.env.REDIRECT_URI || "https://oms-sync.onrender.com/auth/callback";
const TOKEN_FILE = "./token.json";
const SYNC_STATE_FILE = "./sync_state.json";

const SILVER_PRICE_LIST = "gid://shopify/PriceList/32259899698";
const GOLD_PRICE_LIST = "gid://shopify/PriceList/32259866930";
const PLATINUM_PRICE_LIST = "gid://shopify/PriceList/32259834162";

const OMS_API_KEY = "1ae0b8a9-520b-28c2-737b-ec5f52198036";

// ================= BANDWIDTH OPTIMIZATIONS =================
const CONCURRENT_REQUESTS = 15;
const BATCH_UPDATE_SIZE = 50;
const REQUEST_DELAY_MS = 50;
const CACHE_TTL = 300000;

const omsCache = new Map();

// ================= TOKEN MANAGEMENT =================
let tokenData = { access_token: null, expires_at: null, refresh_token: null };
let isRunning = false;
let syncLogs = [];
let priceChanges = [];
let unmatchedSkus = [];
let currentSync = { matched: 0, updated: 0, errors: 0, newProducts: 0, unmatched: 0 };
let lastSyncState = { lastSyncTime: null, lastProductCount: 0, syncedSkus: [] };

// ================= LOAD/SAVE SYNC STATE =================
function loadSyncState() {
    if (fs.existsSync(SYNC_STATE_FILE)) {
        try {
            lastSyncState = JSON.parse(fs.readFileSync(SYNC_STATE_FILE, 'utf8'));
            console.log(`📁 Loaded sync state: ${lastSyncState.syncedSkus.length} SKUs`);
        } catch (err) {}
    }
}

function saveSyncState() {
    try {
        fs.writeFileSync(SYNC_STATE_FILE, JSON.stringify(lastSyncState, null, 2));
    } catch (err) {}
}

// ================= NORMALIZATION =================
function normalizeSku(sku) {
    if (!sku) return "";
    return sku.toUpperCase().trim().replace(/_(BOX|CASE|box|case)$/i, '').replace(/_\d+$/, '').replace(/\s+/g, '').replace(/[-_]+/g, '-').replace(/^[-_]+|[-_]+$/g, '');
}

function getPrice(prices, type) {
    if (!prices || !prices.business_type) return 0;
    const searchType = type.toLowerCase();
    const found = prices.business_type.find(x => x.type && x.type.toLowerCase() === searchType);
    if (!found && searchType === "platinum") {
        const platFound = prices.business_type.find(x => x.type && x.type.toLowerCase() === "platimum");
        return platFound ? Number(platFound.value) : 0;
    }
    return found ? Number(found.value) : 0;
}

function addLog(message, type = "info") {
    const timestamp = new Date().toISOString();
    const logEntry = { timestamp, message, type };
    syncLogs.unshift(logEntry);
    if (syncLogs.length > 200) syncLogs.pop();
    console.log(`[${type.toUpperCase()}] ${message}`);
}

// ================= CACHED OMS LOOKUP =================
async function lookupSkuInOMS(sku) {
    const cacheKey = normalizeSku(sku);
    const cached = omsCache.get(cacheKey);
    if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
        return cached.data;
    }
    
    try {
        const url = `https://api.omspavo.com/product/variants?item_num=${encodeURIComponent(sku)}`;
        const response = await fetch(url, { headers: { "X-Pavo-Token": OMS_API_KEY } });
        
        if (!response.ok) return null;
        
        const data = await response.json();
        if (!data.variants || data.variants.length === 0) return null;
        
        let selectedVariant = data.variants.find(v => v.unit?.type?.toLowerCase() === "piece");
        if (!selectedVariant) selectedVariant = data.variants.find(v => v.unit?.type?.toLowerCase() === "each");
        if (!selectedVariant && data.variants.length > 0) selectedVariant = data.variants[0];
        if (!selectedVariant) return null;
        
        const result = {
            silver: getPrice(selectedVariant.prices, "Silver"),
            gold: getPrice(selectedVariant.prices, "Gold"),
            platinum: getPrice(selectedVariant.prices, "Platinum"),
            originalSku: selectedVariant.sku,
            unit: selectedVariant.unit?.type
        };
        
        omsCache.set(cacheKey, { data: result, timestamp: Date.now() });
        return result;
    } catch (err) {
        return null;
    }
}

// ================= SHOPIFY GRAPHQL =================
async function shopifyGraphQL(query, variables = {}, retryCount = 0) {
    const token = process.env.SHOPIFY_ACCESS_TOKEN;
    if (!token) throw new Error("No token found");

    try {
        const res = await fetch(`https://${SHOP}/admin/api/2025-01/graphql.json`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": token,
            },
            body: JSON.stringify({ query, variables }),
        });

        const json = await res.json();
        if (json.errors) throw new Error(`Shopify Error: ${JSON.stringify(json.errors)}`);
        return json.data;
    } catch (err) {
        if (retryCount < 3) {
            const delay = Math.pow(2, retryCount) * 1000;
            await new Promise(r => setTimeout(r, delay));
            return shopifyGraphQL(query, variables, retryCount + 1);
        }
        throw err;
    }
}

// ================= GET SHOPIFY VARIANTS =================
async function getAllShopifyVariants() {
    addLog("🛍️ Fetching Shopify variants...", "info");
    const variants = [];
    let hasNext = true;
    let cursor = null;
    
    while (hasNext) {
        const query = `query($cursor: String) { 
            productVariants(first: 250, after: $cursor) {
                edges { cursor node { id sku product { id title } } }
                pageInfo { hasNextPage endCursor }
            }
        }`;
        
        const data = await shopifyGraphQL(query, { cursor });
        const edges = data.productVariants?.edges || [];
        
        for (const edge of edges) {
            if (edge.node.sku) {
                variants.push({
                    id: edge.node.id,
                    sku: edge.node.sku,
                    productId: edge.node.product?.id,
                    title: edge.node.product?.title || edge.node.sku
                });
            }
        }
        
        hasNext = data.productVariants?.pageInfo?.hasNextPage || false;
        cursor = data.productVariants?.pageInfo?.endCursor;
    }
    
    addLog(`✅ Total variants: ${variants.length}`, "success");
    return variants;
}

// ================= GET CURRENT PRICES =================
async function getCurrentPriceListPrices(variantId) {
    const query = `
        query($variantId: ID!) {
            silver: priceList(id: "${SILVER_PRICE_LIST}") { priceListPrices(first: 1, variantId: $variantId) { edges { node { price { amount } } } } }
            gold: priceList(id: "${GOLD_PRICE_LIST}") { priceListPrices(first: 1, variantId: $variantId) { edges { node { price { amount } } } } }
            platinum: priceList(id: "${PLATINUM_PRICE_LIST}") { priceListPrices(first: 1, variantId: $variantId) { edges { node { price { amount } } } } }
        }
    `;
    
    try {
        const data = await shopifyGraphQL(query, { variantId });
        return {
            silver: parseFloat(data?.silver?.priceListPrices?.edges?.[0]?.node?.price?.amount || 0),
            gold: parseFloat(data?.gold?.priceListPrices?.edges?.[0]?.node?.price?.amount || 0),
            platinum: parseFloat(data?.platinum?.priceListPrices?.edges?.[0]?.node?.price?.amount || 0)
        };
    } catch (err) {
        return { silver: 0, gold: 0, platinum: 0 };
    }
}

// ================= PROCESS WITH RATE LIMITING =================
async function processConcurrently(items, concurrency, processorFn) {
    const results = [];
    for (let i = 0; i < items.length; i += concurrency) {
        const batch = items.slice(i, i + concurrency);
        const batchResults = await Promise.all(batch.map(processorFn));
        results.push(...batchResults);
        if (REQUEST_DELAY_MS > 0) await new Promise(r => setTimeout(r, REQUEST_DELAY_MS));
    }
    return results;
}

// ================= MAIN SYNC FUNCTION =================
async function runSync() {
    if (isRunning) {
        addLog("⚠️ Sync already in progress", "warning");
        return;
    }
    
    if (!process.env.SHOPIFY_ACCESS_TOKEN) {
        addLog("❌ No Shopify token. Please authenticate at /auth", "error");
        return;
    }
    
    isRunning = true;
    const startTime = Date.now();
    
    currentSync = { startTime, status: "running", shopifyVariants: 0, matched: 0, updated: 0, errors: 0, newProducts: 0, unmatched: 0 };
    
    addLog("=".repeat(50));
    addLog("🚀 STARTING PRICE SYNC");
    addLog("=".repeat(50));
    
    try {
        const shopifyVariants = await getAllShopifyVariants();
        currentSync.shopifyVariants = shopifyVariants.length;
        
        const syncedSkusSet = new Set(lastSyncState.syncedSkus.map(s => normalizeSku(s)));
        const newProducts = shopifyVariants.filter(v => !syncedSkusSet.has(normalizeSku(v.sku)));
        
        addLog(`🎯 Looking up ${shopifyVariants.length} SKUs in OMS...`, "info");
        
        const lookupResults = await processConcurrently(shopifyVariants, CONCURRENT_REQUESTS, async (variant) => {
            const omsPrices = await lookupSkuInOMS(variant.sku);
            return omsPrices ? { ...variant, omsPrices, found: true } : { ...variant, omsPrices: null, found: false };
        });
        
        const foundInOMS = lookupResults.filter(r => r.found);
        const notFound = lookupResults.filter(r => !r.found);
        
        addLog(`✅ Found in OMS: ${foundInOMS.length} | ❌ Not found: ${notFound.length}`, foundInOMS.length > 0 ? "success" : "warning");
        currentSync.unmatched = notFound.length;
        
        if (foundInOMS.length === 0) {
            addLog("❌ No products found in OMS", "error");
            return;
        }
        
        const priceCheckResults = await processConcurrently(foundInOMS, CONCURRENT_REQUESTS, async (item) => {
            const currentPrices = await getCurrentPriceListPrices(item.id);
            const needsUpdate = currentPrices.silver !== item.omsPrices.silver ||
                               currentPrices.gold !== item.omsPrices.gold ||
                               currentPrices.platinum !== item.omsPrices.platinum;
            const isNew = newProducts.some(p => p.sku === item.sku);
            return { ...item, currentPrices, needsUpdate, isNew };
        });
        
        const updates = priceCheckResults.filter(r => r.needsUpdate);
        currentSync.matched = updates.length;
        
        if (updates.length === 0) {
            addLog("✅ All products are in sync!", "success");
            lastSyncState.lastSyncTime = new Date().toISOString();
            lastSyncState.syncedSkus = shopifyVariants.map(v => normalizeSku(v.sku));
            saveSyncState();
            return;
        }
        
        addLog(`💾 Updating ${updates.length} products...`, "info");
        
        let successCount = 0;
        for (let i = 0; i < updates.length; i += BATCH_UPDATE_SIZE) {
            const batch = updates.slice(i, i + BATCH_UPDATE_SIZE);
            
            const updatePriceList = async (listId, getPrice) => {
                const prices = batch.map(item => ({ variantId: item.id, price: { amount: String(getPrice(item)), currencyCode: "USD" } }));
                const mutation = `mutation($id: ID!, $prices: [PriceListPriceInput!]!) { priceListFixedPricesAdd(priceListId: $id, prices: $prices) { userErrors { field message } } }`;
                const result = await shopifyGraphQL(mutation, { id: listId, prices });
                return !result?.priceListFixedPricesAdd?.userErrors?.length;
            };
            
            try {
                const results = await Promise.all([
                    updatePriceList(SILVER_PRICE_LIST, (item) => item.omsPrices.silver),
                    updatePriceList(GOLD_PRICE_LIST, (item) => item.omsPrices.gold),
                    updatePriceList(PLATINUM_PRICE_LIST, (item) => item.omsPrices.platinum)
                ]);
                
                if (results.every(r => r === true)) {
                    successCount += batch.length;
                    for (const item of batch) {
                        priceChanges.unshift({
                            timestamp: new Date().toISOString(),
                            sku: item.sku,
                            title: item.title,
                            oldPrices: item.currentPrices,
                            newPrices: item.omsPrices,
                            isNew: item.isNew
                        });
                        if (priceChanges.length > 200) priceChanges.pop();
                    }
                }
            } catch (err) {
                addLog(`Batch failed: ${err.message}`, "error");
                currentSync.errors += batch.length;
            }
        }
        
        currentSync.updated = successCount;
        
        lastSyncState.lastSyncTime = new Date().toISOString();
        lastSyncState.syncedSkus = shopifyVariants.map(v => normalizeSku(v.sku));
        saveSyncState();
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
        addLog(`✅ SYNC COMPLETED: ${successCount} updated in ${duration}s`, "success");
        
    } catch (err) {
        addLog(`❌ Sync failed: ${err.message}`, "error");
    } finally {
        isRunning = false;
        currentSync.status = "completed";
    }
}

// ================= WEBHOOKS =================
app.post("/webhook/product-update", async (req, res) => {
    addLog("🔔 SHOPIFY: Product update webhook received", "info");
    if (!isRunning) {
        runSync().catch(console.error);
    }
    res.status(200).send("OK");
});

app.post("/webhook/product-create", async (req, res) => {
    addLog("🆕 SHOPIFY: New product creation webhook received!", "success");
    if (!isRunning) {
        runSync().catch(console.error);
    }
    res.status(200).send("OK");
});

app.post("/webhook/product-delete", async (req, res) => {
    addLog("🗑️ SHOPIFY: Product deletion webhook received", "warning");
    res.status(200).send("OK");
});

app.post("/webhooks/oms/product-update", async (req, res) => {
    addLog("⚡ OMS: Product price update webhook received", "success");
    if (!isRunning) {
        runSync().catch(console.error);
    }
    res.status(200).send("OK");
});

// ================= API ENDPOINTS =================
app.post("/sync", async (req, res) => {
    if (isRunning) return res.json({ success: false, error: "Sync already in progress" });
    runSync().catch(console.error);
    res.json({ success: true, message: "Sync started" });
});

app.post("/full-sync", async (req, res) => {
    if (isRunning) return res.json({ success: false, error: "Sync in progress" });
    lastSyncState.syncedSkus = [];
    saveSyncState();
    runSync().catch(console.error);
    res.json({ success: true, message: "Full sync started" });
});

app.get("/sync-status", (req, res) => {
    res.json({
        status: isRunning ? "running" : "idle",
        matched: currentSync?.matched || lastSyncState.syncedSkus.length || 0,
        updated: currentSync?.updated || 0,
        errors: currentSync?.errors || 0,
        newProducts: currentSync?.newProducts || 0,
        unmatched: currentSync?.unmatched || 0,
        lastSyncTime: lastSyncState.lastSyncTime,
        cacheSize: omsCache.size
    });
});

app.get("/health", (req, res) => {
    res.json({
        status: "healthy",
        timestamp: new Date().toISOString(),
        sync: { isRunning: isRunning, lastSync: lastSyncState.lastSyncTime },
        shopify: { authenticated: !!process.env.SHOPIFY_ACCESS_TOKEN },
        cacheSize: omsCache.size
    });
});

app.get("/price-changes", (req, res) => res.json(priceChanges.slice(0, 100)));
app.get("/unmatched-skus", (req, res) => res.json(unmatchedSkus.slice(0, 100)));
app.get("/logs", (req, res) => res.json(syncLogs.slice(0, 100)));

// ================= AUTO-SYNC CONFIGURATION =================
setTimeout(async () => {
    if (process.env.SHOPIFY_ACCESS_TOKEN) {
        addLog("🚀 Auto-starting initial sync in 10 seconds...", "info");
        setTimeout(() => {
            if (!isRunning) {
                runSync().catch(console.error);
            }
        }, 10000);
    }
}, 5000);

cron.schedule("0 */2 * * *", async () => {
    addLog("⏰ Scheduled sync triggered (every 2 hours)", "info");
    if (!isRunning && process.env.SHOPIFY_ACCESS_TOKEN) {
        await runSync();
    }
});

cron.schedule("*/15 * * * *", async () => {
    if (!isRunning && process.env.SHOPIFY_ACCESS_TOKEN) {
        addLog("🔍 Polling OMS for changes...", "info");
        try {
            const checkTime = new Date(Date.now() - 15 * 60 * 1000).toISOString();
            const response = await fetch(`https://api.omspavo.com/products?updated_at_from=${checkTime}&limit=10`, {
                headers: { "X-Pavo-Token": OMS_API_KEY }
            });
            if (response.ok) {
                const data = await response.json();
                if (data.products && data.products.length > 0) {
                    addLog(`📊 OMS has ${data.products.length} updates, syncing...`, "info");
                    runSync().catch(console.error);
                }
            }
        } catch (err) {
            // Silent fail
        }
    }
});

// ================= FRONTEND DASHBOARD =================
app.get("/", (req, res) => {
    res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>OMS → Shopify Price Sync</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0C0C0C; font-family: monospace; padding: 20px; color: #00FF41; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: #1a1a1a; padding: 15px 20px; border: 1px solid #00FF41; border-radius: 8px 8px 0 0; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
        .title { font-size: 18px; font-weight: bold; }
        .btn { background: transparent; border: 1px solid #00FF41; color: #00FF41; padding: 8px 16px; cursor: pointer; font-family: monospace; border-radius: 4px; }
        .btn:hover { background: #00FF41; color: #0C0C0C; }
        .stats { display: grid; grid-template-columns: repeat(6, 1fr); gap: 1px; background: #1a1a1a; }
        .stat { background: #0C0C0C; padding: 15px; text-align: center; }
        .stat-value { font-size: 28px; font-weight: bold; }
        .stat-label { font-size: 10px; color: #00AA33; margin-top: 5px; }
        .panels { display: grid; grid-template-columns: repeat(2, 1fr); gap: 1px; background: #1a1a1a; margin-top: 1px; }
        .panel { background: #0C0C0C; min-height: 400px; display: flex; flex-direction: column; }
        .panel-header { background: #1a1a1a; padding: 10px 15px; border-bottom: 1px solid #00FF41; font-weight: bold; }
        .logs, .changes { flex: 1; overflow-y: auto; padding: 10px; font-size: 11px; max-height: 400px; }
        .log-entry { padding: 4px 0; border-bottom: 1px solid #1a3a1a; }
        .log-info { color: #00AAFF; }
        .log-success { color: #00FF41; }
        .log-warning { color: #FFAA00; }
        .log-error { color: #FF4444; }
        .footer { background: #1a1a1a; padding: 10px 20px; border: 1px solid #00FF41; border-top: none; border-radius: 0 0 8px 8px; display: flex; justify-content: space-between; flex-wrap: wrap; }
        .status-running { color: #00FF41; animation: pulse 1s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <div class="title">> OMS PRICE SYNC <span id="statusBadge">● READY</span></div>
        <div>
            <button class="btn" onclick="startFullSync()">🔄 FULL SYNC</button>
            <button class="btn" onclick="startSync()">⚡ SYNC</button>
            <button class="btn" onclick="clearLogs()">🗑 CLEAR</button>
        </div>
    </div>
    <div class="stats">
        <div class="stat"><div class="stat-value" id="matchedCount">0</div><div class="stat-label">MATCHED</div></div>
        <div class="stat"><div class="stat-value" id="updatedCount">0</div><div class="stat-label">UPDATED</div></div>
        <div class="stat"><div class="stat-value" id="newCount">0</div><div class="stat-label">NEW</div></div>
        <div class="stat"><div class="stat-value" id="unmatchedCount">0</div><div class="stat-label">UNMATCHED</div></div>
        <div class="stat"><div class="stat-value" id="errorCount">0</div><div class="stat-label">ERRORS</div></div>
        <div class="stat"><div class="stat-value" id="cacheSize">0</div><div class="stat-label">CACHE</div></div>
    </div>
    <div class="panels">
        <div class="panel">
            <div class="panel-header">📋 LIVE LOGS</div>
            <div class="logs" id="logs"><div class="log-entry"><span class="log-info">> System ready</span></div></div>
        </div>
        <div class="panel">
            <div class="panel-header">💰 PRICE CHANGES</div>
            <div class="changes" id="changes"><div class="log-entry"><span class="log-info">No changes yet</span></div></div>
        </div>
    </div>
    <div class="footer">
        <span id="health">🟢 HEALTH: OK</span>
        <span id="syncStatusDisplay">SYNC: ⚫ IDLE</span>
        <span id="lastSyncTime">LAST SYNC: NEVER</span>
    </div>
</div>
<script>
    let autoRefreshInterval;
    
    async function startFullSync() {
        addLog('🔄 Triggering FULL SYNC...', 'warning');
        try {
            const response = await fetch('/full-sync', { method: 'POST' });
            const data = await response.json();
            if (data.success) {
                addLog('✅ Full sync started successfully!', 'success');
            } else {
                addLog('❌ Full sync failed: ' + (data.error || 'Unknown error'), 'error');
            }
        } catch (err) {
            addLog('❌ Error starting sync: ' + err.message, 'error');
        }
    }
    
    async function startSync() {
        addLog('⚡ Triggering incremental sync...', 'info');
        try {
            const response = await fetch('/sync', { method: 'POST' });
            const data = await response.json();
            if (data.success) {
                addLog('✅ Sync started successfully!', 'success');
            } else {
                addLog('❌ Sync failed: ' + (data.error || 'Unknown error'), 'error');
            }
        } catch (err) {
            addLog('❌ Error starting sync: ' + err.message, 'error');
        }
    }
    
    function addLog(msg, level) {
        const logsDiv = document.getElementById('logs');
        const logDiv = document.createElement('div');
        logDiv.className = 'log-entry';
        const timestamp = new Date().toLocaleTimeString();
        logDiv.innerHTML = '[' + timestamp + '] <span class="log-' + level + '">' + msg + '</span>';
        logsDiv.insertBefore(logDiv, logsDiv.firstChild);
        if (logsDiv.children.length > 150) logsDiv.removeChild(logsDiv.lastChild);
    }
    
    function clearLogs() {
        document.getElementById('logs').innerHTML = '<div class="log-entry"><span class="log-info">> Logs cleared</span></div>';
        addLog('Logs cleared', 'info');
    }
    
    async function refreshDashboard() {
        try {
            const statusResp = await fetch('/sync-status');
            const status = await statusResp.json();
            
            document.getElementById('matchedCount').innerText = status.matched || 0;
            document.getElementById('updatedCount').innerText = status.updated || 0;
            document.getElementById('newCount').innerText = status.newProducts || 0;
            document.getElementById('unmatchedCount').innerText = status.unmatched || 0;
            document.getElementById('errorCount').innerText = status.errors || 0;
            document.getElementById('cacheSize').innerText = status.cacheSize || 0;
            
            const isRunningStatus = status.status === 'running';
            const statusText = isRunningStatus ? '🟢 RUNNING' : '⚫ IDLE';
            document.getElementById('syncStatusDisplay').innerHTML = 'SYNC: ' + statusText;
            
            if (isRunningStatus) {
                document.getElementById('syncStatusDisplay').className = 'status-running';
                document.getElementById('statusBadge').innerHTML = '● RUNNING';
            } else {
                document.getElementById('syncStatusDisplay').className = '';
                document.getElementById('statusBadge').innerHTML = '● READY';
            }
            
            if (status.lastSyncTime) {
                document.getElementById('lastSyncTime').innerHTML = 'LAST SYNC: ' + new Date(status.lastSyncTime).toLocaleTimeString();
            }
            
            const changesResp = await fetch('/price-changes');
            const changes = await changesResp.json();
            const changesDiv = document.getElementById('changes');
            if (changes.length > 0) {
                let html = '';
                for (let i = 0; i < Math.min(changes.length, 30); i++) {
                    const c = changes[i];
                    html += '<div class="log-entry">📦 <strong>' + c.sku + '</strong> - Silver: $' + c.newPrices.silver + ' (was $' + c.oldPrices.silver + ')</div>';
                }
                changesDiv.innerHTML = html;
            }
            
            const healthResp = await fetch('/health');
            const health = await healthResp.json();
            document.getElementById('health').innerHTML = health.sync.isRunning ? '🟡 SYNC IN PROGRESS' : '🟢 HEALTH: OK';
            
        } catch (err) {
            console.error('Refresh error:', err);
        }
    }
    
    autoRefreshInterval = setInterval(refreshDashboard, 2000);
    refreshDashboard();
    addLog('Dashboard ready - Auto-refresh enabled', 'success');
</script>
</body>
</html>
    `);
});

// ================= AUTH ROUTES =================
app.get("/auth", (req, res) => {
    const url = `https://${SHOP}/admin/oauth/authorize?client_id=${CLIENT_ID}&scope=${SCOPES}&redirect_uri=${REDIRECT_URI}`;
    res.redirect(url);
});

app.get("/auth/callback", async (req, res) => {
    const { code } = req.query;
    try {
        const response = await fetch(`https://${SHOP}/admin/oauth/access_token`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ client_id: CLIENT_ID, client_secret: CLIENT_SECRET, code }),
        });
        const data = await response.json();
        
        if (data.access_token) {
            console.log("\n====================================");
            console.log("COPY THIS TOKEN FOR RENDER:");
            console.log(data.access_token);
            console.log("====================================\n");
            
            res.send(`
                <html><body style="background:#0C0C0C; color:#00FF41; font-family:monospace; padding:50px; text-align:center;">
                <h2>✅ AUTHENTICATION SUCCESSFUL</h2>
                <p>Copy the token above and add it as SHOPIFY_ACCESS_TOKEN in Render environment variables.</p>
                <p>Redirecting to dashboard...</p>
                <script>setTimeout(function() { window.location.href = '/'; }, 3000);</script>
                </body></html>
            `);
        } else {
            res.status(400).send("Auth Failed: " + JSON.stringify(data));
        }
    } catch (err) {
        res.status(500).send("Auth Error: " + err.message);
    }
});

// ================= START SERVER =================
loadSyncState();

setTimeout(function() {
    if (process.env.SHOPIFY_ACCESS_TOKEN) {
        addLog("🔐 Token found - System ready", "success");
        addLog("⚡ Optimized mode: 15 concurrent | 50 batch | 5min cache", "info");
    } else {
        addLog("🔑 No token - Visit /auth to authenticate", "warning");
    }
}, 1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, function() {
    console.log("\n🚀 Server running on port " + PORT);
    console.log("📊 Dashboard: http://localhost:" + PORT + "\n");
});
