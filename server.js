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
// For Render - use environment variable for redirect URI
const REDIRECT_URI = process.env.REDIRECT_URI || "http://localhost:3000/auth/callback";
const TOKEN_FILE = "./token.json";
const SYNC_STATE_FILE = "./sync_state.json";

const SILVER_PRICE_LIST = "gid://shopify/PriceList/32259899698";
const GOLD_PRICE_LIST = "gid://shopify/PriceList/32259866930";
const PLATINUM_PRICE_LIST = "gid://shopify/PriceList/32259834162";

const OMS_API_KEY = "1ae0b8a9-520b-28c2-737b-ec5f52198036";

// ================= PERFORMANCE CONFIG =================
const CONCURRENT_REQUESTS = 30; // Ultra fast - 30 parallel lookups
const BATCH_UPDATE_SIZE = 100;   // Shopify batch update size (max 100)
const REQUEST_DELAY_MS = 10;     // Minimal delay for maximum speed

// ================= TOKEN MANAGEMENT WITH EXPIRY =================
let tokenData = {
    access_token: null,
    expires_at: null,
    refresh_token: null
};

let isRunning = false;
let syncLogs = [];
let priceChanges = [];
let unmatchedSkus = [];
let currentSync = null;
let lastSyncState = { lastSyncTime: null, lastProductCount: 0, syncedSkus: [] };

// ================= LOAD/SAVE SYNC STATE =================

function loadSyncState() {
    if (fs.existsSync(SYNC_STATE_FILE)) {
        try {
            lastSyncState = JSON.parse(fs.readFileSync(SYNC_STATE_FILE, 'utf8'));
            addLog(`📁 Loaded sync state: ${lastSyncState.syncedSkus.length} SKUs previously synced`, "info");
        } catch (err) {
            addLog(`⚠️ Could not load sync state: ${err.message}`, "warning");
        }
    }
}

function saveSyncState() {
    try {
        fs.writeFileSync(SYNC_STATE_FILE, JSON.stringify(lastSyncState, null, 2));
    } catch (err) {
        addLog(`⚠️ Could not save sync state: ${err.message}`, "warning");
    }
}

// ================= NORMALIZATION =================

function normalizeSku(sku) {
    if (!sku) return "";
    return sku
        .toUpperCase()
        .trim()
        .replace(/_(BOX|CASE|box|case)$/i, '')
        .replace(/_\d+$/, '')
        .replace(/\s+/g, '')
        .replace(/[-_]+/g, '-')
        .replace(/^[-_]+|[-_]+$/g, '');
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
    if (syncLogs.length > 500) syncLogs.pop();
    console.log(`[${type.toUpperCase()}] ${message}`);
}

function addPriceChange(sku, title, oldPrices, newPrices, isNew = false) {
    priceChanges.unshift({
        timestamp: new Date().toISOString(),
        sku: sku,
        title: title,
        oldPrices: oldPrices,
        newPrices: newPrices,
        isNew: isNew,
        changes: {
            silver: oldPrices.silver !== newPrices.silver,
            gold: oldPrices.gold !== newPrices.gold,
            platinum: oldPrices.platinum !== newPrices.platinum
        }
    });
    if (priceChanges.length > 500) priceChanges.pop();
}

function addUnmatchedSku(sku, title, reason) {
    unmatchedSkus.unshift({
        timestamp: new Date().toISOString(),
        sku: sku,
        title: title,
        reason: reason,
        normalized: normalizeSku(sku)
    });
    if (unmatchedSkus.length > 500) unmatchedSkus.pop();
}

// ================= TOKEN HELPERS WITH EXPIRY =================

function saveToken(accessToken, expiresIn = null, refreshToken = null) {
    tokenData = {
        access_token: accessToken,
        expires_at: expiresIn ? Date.now() + (expiresIn * 1000) : null,
        refresh_token: refreshToken || null
    };
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokenData));
}

function getToken() {
    return process.env.SHOPIFY_ACCESS_TOKEN || null;
}
async function refreshAccessToken() {
    if (!tokenData.refresh_token) {
        addLog("⚠️ No refresh token available. Please re-authenticate.", "warning");
        return false;
    }
    
    addLog("🔄 Refreshing Shopify access token...", "info");
    
    try {
        const response = await fetch(`https://${SHOP}/admin/oauth/access_token`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                client_id: CLIENT_ID,
                client_secret: CLIENT_SECRET,
                refresh_token: tokenData.refresh_token,
                grant_type: "refresh_token"
            })
        });
        
        const data = await response.json();
        console.log("FULL SHOPIFY RESPONSE:");
console.log(JSON.stringify(data, null, 2));
        
        if (data.access_token) {
            saveToken(data.access_token, data.expires_in, data.refresh_token);
            addLog("✅ Token refreshed successfully!", "success");
            return true;
        } else {
            addLog(`❌ Token refresh failed: ${JSON.stringify(data)}`, "error");
            return false;
        }
    } catch (err) {
        addLog(`❌ Token refresh error: ${err.message}`, "error");
        return false;
    }
}

async function ensureValidToken() {
    const token = getToken();
    if (!token) {
        addLog("❌ No valid token. Please authenticate at /auth", "error");
        return false;
    }
    return true;
}

// ================= SHOPIFY GRAPHQL =================

async function shopifyGraphQL(query, variables = {}, retryCount = 0) {
    const token = getToken();
    if (!token) throw new Error("No token found. Please authenticate first.");

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
            addLog(`Retrying Shopify (${retryCount + 1}/3)...`, "warning");
            await new Promise(r => setTimeout(r, 5000));
            return shopifyGraphQL(query, variables, retryCount + 1);
        }
        throw err;
    }
}

// ================= GET SHOPIFY VARIANTS =================

async function getAllShopifyVariants() {
    addLog("🛍️ Fetching all Shopify variants with SKUs...", "info");
    
    const variants = [];
    let hasNext = true;
    let cursor = null;
    
    while (hasNext) {
        const query = `query($cursor: String) { 
            productVariants(first: 250, after: $cursor) {
                edges { 
                    cursor
                    node { 
                        id 
                        sku
                        product {
                            id
                            title
                        }
                    } 
                }
                pageInfo { 
                    hasNextPage 
                    endCursor 
                }
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
        addLog(`   Fetched ${variants.length} variants with SKUs...`, "info");
    }
    
    addLog(`✅ Total Shopify variants with SKUs: ${variants.length}`, "success");
    return variants;
}

// ================= LOOKUP SKU IN OMS =================

async function lookupSkuInOMS(sku) {
    try {
        const url = `https://api.omspavo.com/product/variants?item_num=${encodeURIComponent(sku)}`;
        const response = await fetch(url, {
            headers: { "X-Pavo-Token": OMS_API_KEY },
        });
        
        if (!response.ok) return null;
        
        const data = await response.json();
        if (!data.variants || data.variants.length === 0) return null;
        
        // Priority order: piece > each > box > case > any variant
        let selectedVariant = null;
        
        // Try to find piece unit first
        selectedVariant = data.variants.find(v => 
            v.unit?.type?.toLowerCase() === "piece"
        );
        
        // If no piece, try "each"
        if (!selectedVariant) {
            selectedVariant = data.variants.find(v => 
                v.unit?.type?.toLowerCase() === "each"
            );
        }
        
        // If still none, take the first variant that's NOT box/case (or just first if all are box/case)
        if (!selectedVariant) {
            selectedVariant = data.variants.find(v => 
                !v.sku.toLowerCase().includes("_box") && 
                !v.sku.toLowerCase().includes("_case") &&
                !v.unit?.type?.toLowerCase()?.includes("box") &&
                !v.unit?.type?.toLowerCase()?.includes("case")
            );
        }
        
        // Last resort: take the first variant
        if (!selectedVariant && data.variants.length > 0) {
            selectedVariant = data.variants[0];
            addLog(`   ⚠️ ${sku}: Using fallback unit type: ${selectedVariant.unit?.type || 'unknown'}`, "warning");
        }
        
        if (!selectedVariant) return null;
        
        const silver = getPrice(selectedVariant.prices, "Silver");
        const gold = getPrice(selectedVariant.prices, "Gold");
        const platinum = getPrice(selectedVariant.prices, "Platinum");
        
        addLog(`   📦 ${sku}: Using unit type "${selectedVariant.unit?.type || 'unknown'}" - S:$${silver} G:$${gold} P:$${platinum}`, "info");
        
        return {
            silver: silver,
            gold: gold,
            platinum: platinum,
            originalSku: selectedVariant.sku,
            unit: selectedVariant.unit?.type
        };
        
    } catch (err) {
        addLog(`   ❌ OMS lookup error for ${sku}: ${err.message}`, "error");
        return null;
    }
}

// ================= GET CURRENT PRICES =================

async function getCurrentPriceListPrices(variantId) {
    try {
        // Query all three price lists in a single GraphQL request
        const query = `
            query($variantId: ID!) {
                silver: priceList(id: "${SILVER_PRICE_LIST}") {
                    priceListPrices(first: 1, variantId: $variantId) {
                        edges {
                            node {
                                price { amount }
                            }
                        }
                    }
                }
                gold: priceList(id: "${GOLD_PRICE_LIST}") {
                    priceListPrices(first: 1, variantId: $variantId) {
                        edges {
                            node {
                                price { amount }
                            }
                        }
                    }
                }
                platinum: priceList(id: "${PLATINUM_PRICE_LIST}") {
                    priceListPrices(first: 1, variantId: $variantId) {
                        edges {
                            node {
                                price { amount }
                            }
                        }
                    }
                }
            }
        `;
        
        const data = await shopifyGraphQL(query, { variantId });
        
        const silverPrice = data?.silver?.priceListPrices?.edges?.[0]?.node?.price?.amount || 0;
        const goldPrice = data?.gold?.priceListPrices?.edges?.[0]?.node?.price?.amount || 0;
        const platinumPrice = data?.platinum?.priceListPrices?.edges?.[0]?.node?.price?.amount || 0;
        
        return {
            silver: parseFloat(silverPrice),
            gold: parseFloat(goldPrice),
            platinum: parseFloat(platinumPrice)
        };
    } catch (err) {
        addLog(`⚠️ Could not fetch prices for ${variantId}: ${err.message}`, "warning");
        return { silver: 0, gold: 0, platinum: 0 };
    }
}

// ================= CONCURRENT PROCESSING =================

async function processConcurrently(items, concurrency, processorFn) {
    const results = [];
    const batches = [];
    
    for (let i = 0; i < items.length; i += concurrency) {
        batches.push(items.slice(i, i + concurrency));
    }
    
    let processed = 0;
    const total = items.length;
    
    for (const batch of batches) {
        const batchResults = await Promise.all(
            batch.map(async (item) => {
                const result = await processorFn(item);
                processed++;
                if (processed % 20 === 0 || processed === total) {
                    addLog(`   Progress: ${processed}/${total} (${Math.round(processed/total*100)}%)`, "info");
                }
                return result;
            })
        );
        results.push(...batchResults);
        
        if (REQUEST_DELAY_MS > 0) {
            await new Promise(r => setTimeout(r, REQUEST_DELAY_MS));
        }
    }
    
    return results;
}
// ================= EFFICIENT OMS POLLING (Only changed products) =================

let lastOmsPollTime = null;

// Load last poll time from file
function loadLastPollTime() {
    const file = "./last_poll.json";
    if (fs.existsSync(file)) {
        try {
            const data = JSON.parse(fs.readFileSync(file, 'utf8'));
            lastOmsPollTime = data.lastPollTime;
            addLog(`📅 Loaded last OMS poll time: ${lastOmsPollTime}`, "info");
        } catch (err) {}
    }
    if (!lastOmsPollTime) {
        lastOmsPollTime = new Date(Date.now() - 5*60000).toISOString();
    }
}

function saveLastPollTime() {
    const file = "./last_poll.json";
    fs.writeFileSync(file, JSON.stringify({ lastPollTime: new Date().toISOString() }, null, 2));
}

// Get only products changed since last poll
async function getChangedOmsProducts() {
    try {
        const fromTime = lastOmsPollTime;
        const url = `https://api.omspavo.com/products?updated_at_from=${encodeURIComponent(fromTime)}&limit=100`;
        
        const response = await fetch(url, {
            headers: { "X-Pavo-Token": OMS_API_KEY }
        });
        
        if (!response.ok) return [];
        
        const data = await response.json();
        return data.products || [];
        
    } catch (err) {
        return [];
    }
}


// Start efficient auto-polling (every 2 hour - only changed products)
function startEfficientPolling() {
    loadLastPollTime();
    
    setInterval(async () => {
    if (!isRunning) {
        addLog("🔄 Auto-poll: Running full comparison sync...", "info");
        await runSync(); // ✅ THIS IS THE FIX - compares all prices
    }
}, 7200000); // 2 hours 
    
    addLog("🔔 Started efficient OMS polling (every 2 hour)", "success");
    addLog("   📅 Only fetches products changed since last poll", "info");
}
// ================= MAIN SYNC FUNCTION =================

async function runSync() {
    if (isRunning) {
        addLog("⚠️ Sync already in progress, skipping...", "warning");
        return;
    }
    
    // Check token before starting
    const isValid = await ensureValidToken();
    if (!isValid) return;
    
    isRunning = true;
    const startTime = Date.now();
    unmatchedSkus = [];
    
    currentSync = {
        startTime: startTime,
        status: "running",
        shopifyVariants: 0,
        matched: 0,
        updated: 0,
        errors: 0,
        newProducts: 0,
        unmatched: 0
    };
    
    addLog("=".repeat(70), "info");
    addLog("🚀 STARTING OMS TO SHOPIFY PRICE SYNC", "info");
    addLog(`⚡ Concurrent lookups: ${CONCURRENT_REQUESTS} parallel requests`, "info");
    addLog("=".repeat(70), "info");
    
    try {
        addLog("\n📡 Step 1: Fetching Shopify variants...", "info");
        const shopifyVariants = await getAllShopifyVariants();
        currentSync.shopifyVariants = shopifyVariants.length;
        addLog(`✅ Fetched ${shopifyVariants.length} Shopify variants`, "success");
        
        const syncedSkusSet = new Set(lastSyncState.syncedSkus.map(s => normalizeSku(s)));
        const newProducts = shopifyVariants.filter(v => !syncedSkusSet.has(normalizeSku(v.sku)));
        
        if (newProducts.length > 0) {
            addLog(`\n🆕 Found ${newProducts.length} NEW products since last sync!`, "success");
            currentSync.newProducts = newProducts.length;
        }
        
        addLog(`\n🎯 Step 2: Looking up ${shopifyVariants.length} SKUs in OMS (${CONCURRENT_REQUESTS} concurrent)...`, "info");
        
        const lookupResults = await processConcurrently(
            shopifyVariants,
            CONCURRENT_REQUESTS,
            async (variant) => {
                const omsPrices = await lookupSkuInOMS(variant.sku);
                
                if (omsPrices) {
                    return { ...variant, omsPrices, found: true };
                } else {
                    addUnmatchedSku(variant.sku, variant.title, "Not found in OMS or no piece unit");
                    return { ...variant, omsPrices: null, found: false };
                }
            }
        );
        
        const foundInOMS = lookupResults.filter(r => r.found);
        const notFound = lookupResults.filter(r => !r.found);
        currentSync.unmatched = notFound.length;
        
        addLog(`\n✅ Found in OMS: ${foundInOMS.length}`, "success");
        addLog(`❌ Not found in OMS: ${notFound.length}`, notFound.length > 0 ? "warning" : "success");
        
        if (foundInOMS.length === 0) {
            addLog("\n❌ No products found in OMS. Check SKU mappings.", "error");
            return;
        }
        
        addLog(`\n💰 Step 3: Checking current Shopify prices for ${foundInOMS.length} products...`, "info");
        
       const priceCheckResults = await processConcurrently(
    foundInOMS,
    CONCURRENT_REQUESTS,
    async (item) => {
        const currentPrices = await getCurrentPriceListPrices(item.id);
        // Check ALL THREE price tiers for changes
        const needsUpdate = 
            currentPrices.silver !== item.omsPrices.silver ||
            currentPrices.gold !== item.omsPrices.gold ||
            currentPrices.platinum !== item.omsPrices.platinum;
        const isNew = newProducts.some(p => p.sku === item.sku);
        
        return { ...item, currentPrices, needsUpdate, isNew };
    }
);
        const updates = priceCheckResults.filter(r => r.needsUpdate);
        currentSync.matched = updates.length;
        
        addLog(`\n📊 Results:`, "info");
        addLog(`   ✅ Products needing update: ${updates.length}`, "success");
        addLog(`   🆕 New products: ${newProducts.length}`, "info");
        addLog(`   ❌ Unmatched SKUs: ${notFound.length}`, "warning");
        
        if (updates.length === 0) {
            addLog("\n✅ All products are in sync with OMS!", "success");
            
            lastSyncState.lastSyncTime = new Date().toISOString();
            lastSyncState.lastProductCount = shopifyVariants.length;
            lastSyncState.syncedSkus = shopifyVariants.map(v => normalizeSku(v.sku));
            saveSyncState();
            
            const duration = ((Date.now() - startTime) / 1000).toFixed(1);
            addLog(`\n⏱️  Total sync time: ${duration} seconds`, "success");
            return;
        }
        
        addLog(`\n💾 Step 4: Bulk updating ${updates.length} products...`, "info");
        
        let successCount = 0;
        let errorCount = 0;
        const totalBatches = Math.ceil(updates.length / BATCH_UPDATE_SIZE);
        
        for (let i = 0; i < updates.length; i += BATCH_UPDATE_SIZE) {
            const batch = updates.slice(i, i + BATCH_UPDATE_SIZE);
            const batchNum = Math.floor(i / BATCH_UPDATE_SIZE) + 1;
            
            addLog(`\n📦 Bulk batch ${batchNum}/${totalBatches} (${batch.length} variants)...`, "info");
                      const updatePriceList = async (listId, getPrice) => {
                const prices = batch.map(item => ({
                    variantId: item.id,
                    price: { amount: String(getPrice(item)), currencyCode: "USD" }
                }));
                
                const mutation = `mutation($id: ID!, $prices: [PriceListPriceInput!]!) { 
                    priceListFixedPricesAdd(priceListId: $id, prices: $prices) { 
                        userErrors { field message } 
                    } 
                }`;
                
                const result = await shopifyGraphQL(mutation, { id: listId, prices });
                if (result?.priceListFixedPricesAdd?.userErrors?.length) {
                    result.priceListFixedPricesAdd.userErrors.forEach(err => {
                        addLog(`   Bulk price error: ${err.message}`, "error");
                    });
                    return false;
                }
                return true;
            };
            
            try {
                const results = await Promise.all([
                    updatePriceList(SILVER_PRICE_LIST, (item) => item.omsPrices.silver),
                    updatePriceList(GOLD_PRICE_LIST, (item) => item.omsPrices.gold),
                    updatePriceList(PLATINUM_PRICE_LIST, (item) => item.omsPrices.platinum)
                ]);
                if (results.every(r => r === true)) {
                    for (const item of batch) {
                        addPriceChange(
                            item.sku,
                            item.title,
                            item.currentPrices,
                            item.omsPrices,
                            item.isNew
                        );
                        successCount++;
                    }
                    addLog(`   ✅ Batch ${batchNum} complete (${successCount}/${updates.length})`, "success");
                } else {
                    errorCount += batch.length;
                }
            } catch (err) {
                errorCount += batch.length;
                addLog(`   ❌ Batch ${batchNum} failed: ${err.message}`, "error");
            }
        }
        
        currentSync.updated = successCount;
        currentSync.errors = errorCount;
        
        lastSyncState.lastSyncTime = new Date().toISOString();
        lastSyncState.lastProductCount = shopifyVariants.length;
        lastSyncState.syncedSkus = shopifyVariants.map(v => normalizeSku(v.sku));
        saveSyncState();
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
        
        addLog("\n" + "=".repeat(70), "info");
        addLog("📊 SYNC COMPLETED", "info");
        addLog("=".repeat(70), "info");
        addLog(`⏱️  Duration: ${duration} seconds (${(duration/60).toFixed(1)} minutes)`, "success");
        addLog(`🛍️ Shopify variants: ${shopifyVariants.length}`, "info");
        addLog(`🆕 New products: ${newProducts.length}`, "info");
        addLog(`✅ Updated: ${successCount}`, "success");
        addLog(`❌ Unmatched SKUs: ${notFound.length}`, notFound.length > 0 ? "warning" : "info");
        addLog(`⚡ Speed: ~${Math.round(shopifyVariants.length / (duration/60))} products/minute`, "success");
        
        if (notFound.length > 0) {
            addLog(`\n📋 First 10 unmatched SKUs:`, "warning");
            notFound.slice(0, 10).forEach(item => {
                addLog(`   - ${item.sku} (${item.title || 'N/A'})`, "warning");
            });
            
            fs.writeFileSync("./unmatched_skus.json", JSON.stringify(notFound.map(s => ({
                sku: s.sku,
                title: s.title,
                normalized: normalizeSku(s.sku)
            })), null, 2));
            addLog(`\n📁 Full unmatched list saved to: unmatched_skus.json`, "info");
        }
        
        addLog("=".repeat(70) + "\n", "info");
        
    } catch (err) {
        addLog(`❌ Sync failed: ${err.message}`, "error");
        console.error(err);
    } finally {
        isRunning = false;
        currentSync.status = "completed";
        currentSync.endTime = Date.now();
    }
}

// ================= WEBHOOK FOR REAL-TIME UPDATES =================

async function registerWebhook() {
    const token = getToken();
    if (!token) return;
    
    // For Render - use your Render URL
    const webhookUrl = process.env.RENDER_URL 
        ? `${process.env.RENDER_URL}/webhook/product-update`
        : "https://your-render-url.onrender.com/webhook/product-update";
    
    addLog(`🔌 Would register webhook at: ${webhookUrl}`, "info");
    addLog("💡 For real-time updates, configure webhook in Shopify admin", "info");
}

// Webhook endpoint for real-time product updates
app.post("/webhook/product-update", async (req, res) => {
    addLog(`🔔 Product update webhook received!`, "info");
    // Trigger sync for just this product if needed
    res.status(200).send("OK");
});

app.post("/webhook/product-create", async (req, res) => {
    addLog(`🆕 New product creation webhook received!`, "success");
    // Trigger full sync to pick up new product
    if (!isRunning) {
        addLog("🔄 Triggering sync for new product...", "info");
        runSync().catch(console.error);
    }
    res.status(200).send("OK");
});
// ================= OMS WEBHOOKS FOR INSTANT SYNC =================

// OMS Product Update Webhook - Called by OMS when ANY product changes
app.post("/webhooks/oms/product-update", async (req, res) => {
    addLog(`⚡ INSTANT: OMS product update webhook received!`, "success");
    const { item_num, sku, updated_at } = req.body;
    
    const targetSku = sku || item_num;
    if (targetSku) {
        addLog(`   → Instantly syncing SKU: ${targetSku}`, "info");
        // Sync just this one SKU immediately
        syncSingleSku(targetSku, "OMS webhook").catch(console.error);
    } else {
        addLog(`   ⚠️ No SKU provided, running quick check`, "warning");
        const recentSkus = await getRecentOmsChanges();
        if (recentSkus.length > 0) {
            for (const sku of recentSkus.slice(0, 5)) {
                await syncSingleSku(sku, "batch webhook");
            }
        }
    }
    
    res.status(200).send("OK");
});

// OMS Price Sheet Webhook - For price-specific changes
app.post("/webhooks/oms/price-change", async (req, res) => {
    addLog(`💰 INSTANT: OMS price change webhook received!`, "success");
    const { item_num, sku, price_sheet_id, value } = req.body;
    
    const targetSku = sku || item_num;
    if (targetSku) {
        addLog(`   → Instantly updating price for: ${targetSku}`, "info");
        syncSingleSku(targetSku, "OMS price change").catch(console.error);
    }
    
    res.status(200).send("OK");
});



// Single SKU sync function for instant updates
async function syncSingleSku(sku, source = "webhook") {
    if (isRunning) {
        addLog(`⏭️ Deferring ${sku} - full sync in progress`, "warning");
        return;
    }
    
    addLog(`⚡ INSTANT SYNC: ${sku} from ${source}`, "info");
    
    try {
        // Find Shopify variant by SKU
        const query = `
            query($sku: String!) {
                productVariants(first: 1, query: $sku) {
                    edges {
                        node {
                            id
                            product { title }
                        }
                    }
                }
            }
        `;
        
        const data = await shopifyGraphQL(query, { sku: `sku:'${sku}'` });
        const variant = data?.productVariants?.edges?.[0]?.node;
        
        if (!variant) {
            addLog(`❌ SKU ${sku} not found in Shopify`, "error");
            return;
        }
        
        // Get OMS prices
        const omsPrices = await lookupSkuInOMS(sku);
        if (!omsPrices) {
            addLog(`❌ SKU ${sku} not found in OMS`, "error");
            return;
        }
        
        // Get current Shopify prices
        const currentPrices = await getCurrentPriceListPrices(variant.id);
        
        // Check if update needed
        if (currentPrices.silver === omsPrices.silver && 
            currentPrices.gold === omsPrices.gold && 
            currentPrices.platinum === omsPrices.platinum) {
            addLog(`✅ SKU ${sku} prices already match OMS`, "success");
            return;
        }
        
        // Update all three price lists
               const updatePriceList = async (listId, price) => {
            const mutation = `
                mutation($id: ID!, $prices: [PriceListPriceInput!]!) {
                    priceListFixedPricesAdd(priceListId: $id, prices: $prices) {
                        userErrors { field message }
                    }
                }
            `;
            
            const result = await shopifyGraphQL(mutation, {
                id: listId,
                prices: [{
                    variantId: variant.id,
                    price: { amount: String(price), currencyCode: "USD" }
                }]
            });
            
            if (result?.priceListFixedPricesAdd?.userErrors?.length) {
                return false;
            }
            return true;
        };
        
        const results = await Promise.all([
            updatePriceList(SILVER_PRICE_LIST, omsPrices.silver),
            updatePriceList(GOLD_PRICE_LIST, omsPrices.gold),
            updatePriceList(PLATINUM_PRICE_LIST, omsPrices.platinum)
        ]);
        
        if (results.every(r => r === true)) {
            addPriceChange(sku, variant.product?.title || sku, currentPrices, omsPrices);
            addLog(`✅ INSTANT SYNC COMPLETE: ${sku} updated`, "success");
        } else {
            addLog(`❌ Failed to update ${sku}`, "error");
        }
        
    } catch (err) {
        addLog(`❌ Instant sync failed for ${sku}: ${err.message}`, "error");
    }
}
// ================= DETECT AND REVERT MANUAL PRICE CHANGES =================

// Track known prices to detect manual changes
let knownPrices = new Map(); // SKU -> { silver, gold, platinum, lastSeen }

// Load known prices from file
function loadKnownPrices() {
    const file = "./known_prices.json";
    if (fs.existsSync(file)) {
        try {
            const data = JSON.parse(fs.readFileSync(file, 'utf8'));
            knownPrices = new Map(Object.entries(data));
            addLog(`📁 Loaded ${knownPrices.size} known SKU prices`, "info");
        } catch (err) {}
    }
}

// Save known prices to file
function saveKnownPrices() {
    const file = "./known_prices.json";
    const obj = Object.fromEntries(knownPrices);
    fs.writeFileSync(file, JSON.stringify(obj, null, 2));
}

// Check for manual price changes in Shopify
async function detectManualPriceChanges() {
    addLog("🔍 Scanning for manual price changes in Shopify...", "info");
    
    try {
        // Fetch all Shopify variants (limit to 50 per check to avoid overloading)
        const shopifyVariants = await getAllShopifyVariants();
        let manualChangesDetected = 0;
        let checked = 0;
        
        for (const variant of shopifyVariants) {
            const sku = variant.sku;
            if (!sku) continue;
            
            checked++;
            if (checked % 10 === 0) {
                addLog(`   Progress: ${checked}/${Math.min(shopifyVariants.length, 50)}`, "info");
            }
            
            // Get current prices from Shopify
            const currentPrices = await getCurrentPriceListPrices(variant.id);
            
            // Get OMS prices
            const omsPrices = await lookupSkuInOMS(sku);
            if (!omsPrices) continue;
            
            // Check if current prices match OMS
            const matchesOMS = 
                currentPrices.silver === omsPrices.silver &&
                currentPrices.gold === omsPrices.gold &&
                currentPrices.platinum === omsPrices.platinum;
            
            if (!matchesOMS) {
                // Manual change detected! Force revert
                addLog(`⚠️ MANUAL PRICE CHANGE DETECTED for ${sku}`, "warning");
                addLog(`   Current: S$${currentPrices.silver} G$${currentPrices.gold} P$${currentPrices.platinum}`, "warning");
                addLog(`   OMS: S$${omsPrices.silver} G$${omsPrices.gold} P$${omsPrices.platinum}`, "info");
                addLog(`   🔄 Force reverting to OMS prices...`, "info");
                
                // Update price lists to OMS values
                                const updatePriceList = async (listId, price) => {
                    const mutation = `
                        mutation($id: ID!, $prices: [PriceListPriceInput!]!) {
                            priceListFixedPricesAdd(priceListId: $id, prices: $prices) {
                                userErrors { field message }
                            }
                        }
                    `;
                    
                    const result = await shopifyGraphQL(mutation, {
                        id: listId,
                        prices: [{
                            variantId: variant.id,
                            price: { amount: String(price), currencyCode: "USD" }
                        }]
                    });
                    
                    return !result?.priceListFixedPricesAdd?.userErrors?.length;
                };
                
                const results = await Promise.all([
                    updatePriceList(SILVER_PRICE_LIST, omsPrices.silver),
                    updatePriceList(GOLD_PRICE_LIST, omsPrices.gold),
                    updatePriceList(PLATINUM_PRICE_LIST, omsPrices.platinum)
                ]);
                
                if (results.every(r => r === true)) {
                    addLog(`   ✅ Force reverted ${sku} back to OMS prices`, "success");
                    manualChangesDetected++;
                    
                    // Record the forced change
                    addPriceChange(sku, variant.title || sku, currentPrices, omsPrices, false);
                } else {
                    addLog(`   ❌ Failed to revert ${sku}`, "error");
                }
                
                // Small delay to avoid rate limits
                await new Promise(r => setTimeout(r, 200));
            }
        }
        
        if (manualChangesDetected > 0) {
            addLog(`📊 Detected and reverted ${manualChangesDetected} manual price change(s)`, "success");
        } else {
            addLog("✅ No manual price changes detected", "success");
        }
        
        saveKnownPrices();
        
    } catch (err) {
        addLog(`❌ Manual price detection failed: ${err.message}`, "error");
    }
}

// Start manual price monitoring (every 5 minutes)
function startManualPriceMonitoring() {
    loadKnownPrices();
    
    setInterval(async () => {
        if (!isRunning) {
            await detectManualPriceChanges();
        }
    }, 300000); // 5 minutes
    
    addLog("🔔 Started manual price change monitoring (every 5 minutes)", "success");
    addLog("   📝 Any manual price changes will be forcefully reverted to OMS", "warning");
}

// ================= MANUAL PRICE CHECK ENDPOINT =================
app.post("/check-manual-prices", async (req, res) => {
    if (isRunning) {
        return res.json({ success: false, error: "Sync already in progress" });
    }
    
    addLog("🔍 Manual price check triggered", "info");
    detectManualPriceChanges().catch(console.error);
    res.json({ success: true, message: "Manual price check started" });
});
// Helper: Get recent OMS changes
async function getRecentOmsChanges() {
    try {
        const fromTime = new Date(Date.now() - 5*60000).toISOString();
        const response = await fetch(`https://api.omspavo.com/products?updated_at_from=${fromTime}&limit=20`, {
            headers: { "X-Pavo-Token": OMS_API_KEY }
        });
        if (!response.ok) return [];
        const data = await response.json();
        return (data.products || []).map(p => p.item_num).filter(Boolean);
    } catch (err) {
        return [];
    }
}
// ================= SHOPIFY WEBHOOKS FOR AUTO-SYNC =================
app.post("/webhooks/shopify/product-create", async (req, res) => {
    console.log("🔥 WEBHOOK HIT (RENDER)");
    console.log(JSON.stringify(req.body, null, 2));

    res.status(200).send("OK");
});
// Shopify Product Create Webhook - Auto-sync when you add NEW product in Shopify
app.post("/webhooks/shopify/product-create", async (req, res) => {
    addLog(`🛍️ SHOPIFY: New product creation webhook received!`, "success");
    
    try {
        const product = req.body;
        const variant = product.variants?.[0];
        const sku = variant?.sku;
        
        if (!sku) {
            addLog(`   ⚠️ Product has no SKU, skipping auto-sync`, "warning");
            return res.status(200).send("OK");
        }
        
        addLog(`   → New product detected: ${sku} - ${product.title}`, "info");
        
        // Look up SKU in OMS
        const omsPrices = await lookupSkuInOMS(sku);
        
        if (!omsPrices) {
            addLog(`   ❌ SKU ${sku} not found in OMS - check OMS or add piece unit`, "warning");
            addUnmatchedSku(sku, product.title, "Not found in OMS when product created in Shopify");
            return res.status(200).send("OK");
        }
        
        addLog(`   ✅ SKU found in OMS! Updating prices...`, "success");
        addLog(`   📦 OMS Prices - Silver: $${omsPrices.silver}, Gold: $${omsPrices.gold}, Platinum: $${omsPrices.platinum}`, "info");
        
        // Find the variant ID in Shopify
        const query = `
            query($sku: String!) {
                productVariants(first: 1, query: $sku) {
                    edges {
                        node {
                            id
                            product { title }
                        }
                    }
                }
            }
        `;
        
        const data = await shopifyGraphQL(query, { sku: `sku:'${sku}'` });
        const variantNode = data?.productVariants?.edges?.[0]?.node;
        
        if (!variantNode) {
            addLog(`   ❌ Could not find variant for SKU ${sku} after creation`, "error");
            return res.status(200).send("OK");
        }
        
        // Update all three price lists
        const updatePriceList = async (listId, price) => {
            const mutation = `
                mutation($id: ID!, $prices: [PriceListPriceInput!]!) {
                    priceListFixedPricesAdd(priceListId: $id, prices: $prices) {
                        userErrors { field message }
                    }
                }
            `;
            
            const result = await shopifyGraphQL(mutation, {
                id: listId,
                prices: [{
                    variantId: variantNode.id,
                    price: { amount: String(price), currencyCode: "USD" }
                }]
            });
            
            if (result?.priceListFixedPricesAdd?.userErrors?.length) {
                result.priceListFixedPricesAdd.userErrors.forEach(err => {
                    addLog(`      Error: ${err.message}`, "error");
                });
                return false;
            }
            return true;
        };
        
        const results = await Promise.all([
            updatePriceList(SILVER_PRICE_LIST, omsPrices.silver),
            updatePriceList(GOLD_PRICE_LIST, omsPrices.gold),
            updatePriceList(PLATINUM_PRICE_LIST, omsPrices.platinum)
        ]);
        
        if (results.every(r => r === true)) {
            const currentPrices = { silver: 0, gold: 0, platinum: 0 };
            addPriceChange(sku, variantNode.product?.title || sku, currentPrices, omsPrices, true);
            addLog(`   ✅ AUTO-SYNC COMPLETE: ${sku} prices set from OMS`, "success");
        } else {
            addLog(`   ❌ Failed to update some price lists for ${sku}`, "error");
        }
        
    } catch (err) {
        addLog(`   ❌ Auto-sync failed: ${err.message}`, "error");
    }
    
    res.status(200).send("OK");
});

// Shopify Product Update Webhook - Auto-sync when product is UPDATED in Shopify
app.post("/webhooks/shopify/product-update", async (req, res) => {
    addLog(`🛍️ SHOPIFY: Product update webhook received!`, "info");
    
    try {
        const product = req.body;
        const variant = product.variants?.[0];
        const sku = variant?.sku;
        
        if (!sku) {
            addLog(`   ⚠️ Updated product has no SKU, skipping sync`, "warning");
            return res.status(200).send("OK");
        }
        
        addLog(`   → Product updated: ${sku} - ${product.title}`, "info");
        
        // Look up SKU in OMS
        const omsPrices = await lookupSkuInOMS(sku);
        
        if (!omsPrices) {
            addLog(`   ⚠️ SKU ${sku} not found in OMS - check OMS`, "warning");
            addUnmatchedSku(sku, product.title, "Updated in Shopify but not found in OMS");
            return res.status(200).send("OK");
        }
        
        // Get current Shopify prices
        const query = `
            query($sku: String!) {
                productVariants(first: 1, query: $sku) {
                    edges {
                        node {
                            id
                            product { title }
                        }
                    }
                }
            }
        `;
        
        const data = await shopifyGraphQL(query, { sku: `sku:'${sku}'` });
        const variantNode = data?.productVariants?.edges?.[0]?.node;
        
        if (!variantNode) {
            addLog(`   ❌ Could not find variant for SKU ${sku}`, "error");
            return res.status(200).send("OK");
        }
        
        // Get current prices from Shopify
        const currentPrices = await getCurrentPriceListPrices(variantNode.id);
        
        // Check if update needed
        const needsUpdate = 
            currentPrices.silver !== omsPrices.silver ||
            currentPrices.gold !== omsPrices.gold ||
            currentPrices.platinum !== omsPrices.platinum;
        
        if (!needsUpdate) {
            addLog(`   ✅ ${sku} prices already match OMS`, "success");
            return res.status(200).send("OK");
        }
        
        addLog(`   🔄 Prices differ from OMS - updating...`, "info");
        addLog(`   Current: S$${currentPrices.silver} G$${currentPrices.gold} P$${currentPrices.platinum}`, "info");
        addLog(`   OMS: S$${omsPrices.silver} G$${omsPrices.gold} P$${omsPrices.platinum}`, "info");
        
        // Update all three price lists
        const updatePriceList = async (listId, price) => {
            const mutation = `
                mutation($id: ID!, $prices: [PriceListPriceInput!]!) {
                    priceListFixedPricesAdd(priceListId: $id, prices: $prices) {
                        userErrors { field message }
                    }
                }
            `;
            
            const result = await shopifyGraphQL(mutation, {
                id: listId,
                prices: [{
                    variantId: variantNode.id,
                    price: { amount: String(price), currencyCode: "USD" }
                }]
            });
            
            if (result?.priceListFixedPricesAdd?.userErrors?.length) {
                result.priceListFixedPricesAdd.userErrors.forEach(err => {
                    addLog(`      Error: ${err.message}`, "error");
                });
                return false;
            }
            return true;
        };
        
        const results = await Promise.all([
            updatePriceList(SILVER_PRICE_LIST, omsPrices.silver),
            updatePriceList(GOLD_PRICE_LIST, omsPrices.gold),
            updatePriceList(PLATINUM_PRICE_LIST, omsPrices.platinum)
        ]);
        
        if (results.every(r => r === true)) {
            addPriceChange(sku, variantNode.product?.title || sku, currentPrices, omsPrices, false);
            addLog(`   ✅ AUTO-SYNC COMPLETE: ${sku} prices updated from OMS`, "success");
        } else {
            addLog(`   ❌ Failed to update some price lists for ${sku}`, "error");
        }
        
    } catch (err) {
        addLog(`   ❌ Auto-sync failed: ${err.message}`, "error");
    }
    
    res.status(200).send("OK");
});

// Optional: Shopify Product Delete Webhook - Track deleted products
app.post("/webhooks/shopify/product-delete", async (req, res) => {
    addLog(`🛍️ SHOPIFY: Product deleted webhook received`, "warning");
    const { id } = req.body;
    addLog(`   → Product ID: ${id} was deleted`, "warning");
    // You can add logic here to track deletions if needed
    res.status(200).send("OK");
});
// ================= API ENDPOINTS =================

app.get("/unmatched-skus", (req, res) => {
    res.json(unmatchedSkus.slice(0, 200));
});

app.get("/price-changes", (req, res) => {
    res.json(priceChanges.slice(0, 200));
});

app.post("/sync", async (req, res) => {
    if (isRunning) {
        return res.json({ success: false, error: "Sync already in progress" });
    }
    runSync().catch(console.error);
    res.json({ success: true, message: "Sync started" });
});
// ================= FULL SYNC ENDPOINT =================
// ================= FULL SYNC ENDPOINT =================
app.post("/full-sync", async (req, res) => {
    if (isRunning) {
        return res.json({ success: false, error: "Sync already in progress" });
    }
    
    addLog("📦 FULL SYNC MODE - Will sync all products regardless of previous sync", "warning");
    
    // Force clear sync state to sync all products
    const originalSyncedSkus = [...lastSyncState.syncedSkus];
    lastSyncState.syncedSkus = [];
    addLog("📋 Cleared sync history - will sync ALL products", "info");
    
    // Run full sync in background
    runSync().catch(console.error);
    
    res.json({ success: true, message: "Full sync started" });
});
// ================= INCREMENTAL SYNC ENDPOINT =================
// ================= INCREMENTAL SYNC ENDPOINT =================
app.post("/incremental-sync", async (req, res) => {
    if (isRunning) {
        return res.json({ success: false, error: "Sync already in progress" });
    }
    
    addLog("⚡ INCREMENTAL SYNC MODE - Running full comparison sync", "info");
    runSync().catch(console.error);  // ✅ Use runSync instead
    res.json({ success: true, message: "Incremental sync started" });
});

app.get("/sync-stream", (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    
    const sendStats = () => {
        const stats = {
            matched: currentSync?.matched || lastSyncState.syncedSkus.length || 0,
            updated: currentSync?.updated || 0,
            errors: currentSync?.errors || 0,
            newProducts: currentSync?.newProducts || 0,
            unmatched: currentSync?.unmatched || 0,
            status: isRunning ? "running" : (currentSync?.status || "idle"),
            lastSyncTime: lastSyncState.lastSyncTime
        };
        res.write(`data: ${JSON.stringify({ type: 'stats', stats })}\n\n`);
    };
    
    const interval = setInterval(sendStats, 2000);
    req.on('close', () => clearInterval(interval));
});

app.get("/sync-status", (req, res) => {
    res.json({
        matched: currentSync?.matched || lastSyncState.syncedSkus.length || 0,
        updated: currentSync?.updated || 0,
        errors: currentSync?.errors || 0,
        newProducts: currentSync?.newProducts || 0,
        unmatched: currentSync?.unmatched || 0,
        status: isRunning ? "running" : (currentSync?.status || "idle"),
        lastSyncTime: lastSyncState.lastSyncTime
    });
});

// ================= HEALTH CHECK =================

app.get("/health", (req, res) => {
    const uptime = process.uptime();
    const uptimeHours = Math.floor(uptime / 3600);
    const uptimeMinutes = Math.floor((uptime % 3600) / 60);
    const uptimeSeconds = Math.floor(uptime % 60);
    
    res.json({
        status: "healthy",
        timestamp: new Date().toISOString(),
        uptime: `${uptimeHours}h ${uptimeMinutes}m ${uptimeSeconds}s`,
        sync: {
            isRunning: isRunning,
            lastSync: lastSyncState.lastSyncTime,
            lastSyncStats: currentSync ? {
                matched: currentSync.matched,
                updated: currentSync.updated,
                errors: currentSync.errors,
                newProducts: currentSync.newProducts,
                unmatched: currentSync.unmatched
            } : null
        },
        shopify: { authenticated: !!getToken(), shop: SHOP },
        oms: { apiConfigured: !!OMS_API_KEY },
        syncState: {
            productsSynced: lastSyncState.syncedSkus.length,
            lastSyncTime: lastSyncState.lastSyncTime
        }
    });
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
                body { background: #0C0C0C; font-family: 'Consolas', monospace; padding: 20px; color: #00FF41; }
                .terminal { max-width: 1600px; margin: 0 auto; background: #0C0C0C; border: 1px solid #00FF41; border-radius: 8px; overflow: hidden; }
                .terminal-header { background: #1a1a1a; padding: 12px 20px; border-bottom: 1px solid #00FF41; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
                .terminal-title { color: #00FF41; font-size: 14px; font-weight: bold; }
                .terminal-title::before { content: "> "; }
                .button-group { display: flex; gap: 10px; flex-wrap: wrap; }
                .cmd-btn { background: transparent; border: 1px solid #00FF41; color: #00FF41; padding: 6px 15px; font-family: monospace; font-size: 12px; cursor: pointer; transition: all 0.2s; }
                .cmd-btn:hover { background: #00FF41; color: #0C0C0C; }
                .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 1px; background: #1a1a1a; border-bottom: 1px solid #00FF41; }
                .stat-card { background: #0C0C0C; padding: 15px; text-align: center; }
                .stat-value { font-size: 28px; font-weight: bold; color: #00FF41; }
                .stat-label { font-size: 10px; color: #00AA33; margin-top: 5px; text-transform: uppercase; }
                .panels { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1px; background: #1a1a1a; }
                .panel { background: #0C0C0C; min-height: 400px; display: flex; flex-direction: column; }
                .panel-header { background: #1a1a1a; padding: 10px 15px; border-bottom: 1px solid #00FF41; font-size: 12px; font-weight: bold; }
                .logs-container, .changes-container, .unmatched-container { flex: 1; overflow-y: auto; padding: 10px; font-size: 11px; max-height: 500px; }
                .log-entry { padding: 4px 0; border-bottom: 1px solid #1a3a1a; font-family: monospace; }
                .log-info { color: #00AAFF; }
                .log-success { color: #00FF41; }
                .log-warning { color: #FFAA00; }
                .log-error { color: #FF4444; }
                .change-entry, .unmatched-entry { padding: 8px; border-bottom: 1px solid #1a3a1a; }
                .change-sku, .unmatched-sku { color: #00FF41; font-weight: bold; }
                .price-change { margin-top: 5px; padding-left: 10px; color: #00FF41; }
                .badge-new { background: #00FF41; color: #0C0C0C; padding: 2px 6px; border-radius: 4px; font-size: 9px; margin-left: 5px; }
                .health-check { background: #1a1a1a; padding: 10px 15px; border-top: 1px solid #00FF41; font-size: 11px; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
                @media (max-width: 1024px) { .panels { grid-template-columns: 1fr; } }
                ::-webkit-scrollbar { width: 8px; }
                ::-webkit-scrollbar-track { background: #1a1a1a; }
                ::-webkit-scrollbar-thumb { background: #00FF41; border-radius: 4px; }
            </style>
        </head>
        <body>
            <div class="terminal">
                <div class="terminal-header">
                    <div class="terminal-title">OMS SYNC [READY]</div>
               <div class="button-group">
    <button class="cmd-btn" onclick="startFullSync()" style="background:#00AA33; color:#0C0C0C;">🔄 FULL SYNC</button>
    <button class="cmd-btn" onclick="startSync()">⚡ INCREMENTAL SYNC</button>
    <button class="cmd-btn" onclick="checkManualPrices()">🔍 CHECK MANUAL PRICES</button>
    <button class="cmd-btn" onclick="window.location.href='/auth'">🔑 AUTH</button>
    <button class="cmd-btn" onclick="clearAll()">🗑 CLEAR ALL</button>
    <button class="cmd-btn" onclick="refreshHealth()">🔄 HEALTH</button>
</div>
                </div>
                
                <div class="stats-grid">
                    <div class="stat-card"><div class="stat-value" id="syncedCount">0</div><div class="stat-label">SYNCED SKUs</div></div>
                    <div class="stat-card"><div class="stat-value" id="updatedCount">0</div><div class="stat-label">UPDATED</div></div>
                    <div class="stat-card"><div class="stat-value" id="newCount">0</div><div class="stat-label">NEW PRODUCTS</div></div>
                    <div class="stat-card"><div class="stat-value" id="unmatchedCount">0</div><div class="stat-label">UNMATCHED</div></div>
                    <div class="stat-card"><div class="stat-value" id="errorCount">0</div><div class="stat-label">ERRORS</div></div>
                </div>
                
                <div class="panels">
                    <div class="panel">
                        <div class="panel-header">📋 SYNC_LOGS</div>
                        <div class="logs-container" id="logs">
                            <div class="log-entry"><span class="log-info">> OMS SYNC READY</span></div>
                            <div class="log-entry"><span class="log-info">> 30 concurrent requests | 100 products/batch</span></div>
                        </div>
                    </div>
                    <div class="panel">
                        <div class="panel-header">💰 PRICE CHANGES & NEW PRODUCTS</div>
                        <div class="changes-container" id="changes">
                            <div class="change-entry"><span class="log-info">No changes yet</span></div>
                        </div>
                    </div>
                    <div class="panel">
                        <div class="panel-header">❌ UNMATCHED SKUs</div>
                        <div class="unmatched-container" id="unmatched">
                            <div class="unmatched-entry"><span class="log-info">No unmatched SKUs</span></div>
                        </div>
                    </div>
                </div>
                
                <div class="health-check">
                    <span id="healthStatus">🟢 HEALTH: OK</span>
                    <span id="uptime">UPTIME: 0s</span>
                    <span id="syncStatus">SYNC: IDLE</span>
                    <span id="lastSync">LAST SYNC: NEVER</span>
                </div>
            </div>
            
            <script>
                let eventSource;
                
                function connectSSE() {
                    if (eventSource) eventSource.close();
                    eventSource = new EventSource('/sync-stream');
                    eventSource.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        if (data.type === 'log') addLog(data.message, data.level);
                        else if (data.type === 'stats') updateStats(data.stats);
                    };
                }
                
                function addLog(message, level) {
                    const logsDiv = document.getElementById('logs');
                    const timestamp = new Date().toLocaleTimeString();
                    const logDiv = document.createElement('div');
                    logDiv.className = 'log-entry';
                    logDiv.innerHTML = \`[\${timestamp}] <span class="log-\${level}">\${message}</span>\`;
                    logsDiv.insertBefore(logDiv, logsDiv.firstChild);
                    if (logsDiv.children.length > 200) logsDiv.removeChild(logsDiv.lastChild);
                }
                
                function updateStats(stats) {
                    document.getElementById('syncedCount').textContent = stats.matched || 0;
                    document.getElementById('updatedCount').textContent = stats.updated || 0;
                    document.getElementById('newCount').textContent = stats.newProducts || 0;
                    document.getElementById('unmatchedCount').textContent = stats.unmatched || 0;
                    document.getElementById('errorCount').textContent = stats.errors || 0;
                    document.getElementById('syncStatus').innerHTML = stats.status === 'running' ? 'SYNC: 🟢 RUNNING' : 'SYNC: ⚫ IDLE';
                    if (stats.lastSyncTime) document.getElementById('lastSync').innerHTML = 'LAST SYNC: ' + new Date(stats.lastSyncTime).toLocaleTimeString();
                    
                    if (stats.status === 'running') {
                        document.querySelector('.terminal-title').innerHTML = '> OMS SYNC [RUNNING]';
                    } else {
                        document.querySelector('.terminal-title').innerHTML = '> OMS SYNC [READY]';
                    }
                }
                
            async function startSync() {
    addLog('⚡ INCREMENTAL_SYNC_TRIGGERED', 'info');
    const response = await fetch('/incremental-sync', { method: 'POST' });
    const data = await response.json();
    if (data.success) {
        addLog('INCREMENTAL_SYNC_STARTED - Syncing changed products only', 'success');
    } else {
        addLog('SYNC_FAILED: ' + data.error, 'error');
    }
}

async function startFullSync() {
    addLog('🔄 FULL_SYNC_TRIGGERED - This will sync ALL products', 'warning');
    addLog('⚠️ This may take 5-10 minutes for 2000+ products', 'warning');
    const response = await fetch('/full-sync', { method: 'POST' });
    const data = await response.json();
    if (data.success) {
        addLog('FULL_SYNC_STARTED - Syncing all products...', 'success');
        // Force refresh stats
        setTimeout(() => fetchStats(), 1000);
    } else {
        addLog('FULL_SYNC_FAILED: ' + data.error, 'error');
    }
}
                async function checkManualPrices() {
    addLog('🔍 CHECKING FOR MANUAL PRICE CHANGES...', 'info');
    const response = await fetch('/check-manual-prices', { method: 'POST' });
    const data = await response.json();
    if (data.success) {
        addLog('✅ Manual price check completed. Any changes will be reverted.', 'success');
    } else {
        addLog('❌ Check failed: ' + data.error, 'error');
    }
}
                
                async function loadPriceChanges() {
                    const response = await fetch('/price-changes');
                    const changes = await response.json();
                    const changesDiv = document.getElementById('changes');
                    changesDiv.innerHTML = '';
                    if (changes.length === 0) {
                        changesDiv.innerHTML = '<div class="change-entry"><span class="log-info">No changes yet</span></div>';
                    } else {
                        changes.slice(0, 50).forEach(change => {
                            const div = document.createElement('div');
                            div.className = 'change-entry';
                            div.innerHTML = \`
    <div class="change-sku">📦 \${change.sku} \${change.isNew ? '<span class="badge-new">NEW</span>' : ''}</div>
    <div class="change-title">\${change.title || change.sku}</div>
    <div class="price-change">💰 Silver: $\${change.newPrices.silver} (was $\${change.oldPrices.silver})</div>
    <div class="price-change">🥇 Gold: $\${change.newPrices.gold} (was $\${change.oldPrices.gold})</div>
    <div class="price-change">💎 Platinum: $\${change.newPrices.platinum} (was $\${change.oldPrices.platinum})</div>
    <div style="color:#555;font-size:9px; margin-top: 5px;">\${new Date(change.timestamp).toLocaleString()}</div>
\`;
                            changesDiv.appendChild(div);
                        });
                    }
                }
                
                async function loadUnmatched() {
                    const response = await fetch('/unmatched-skus');
                    const unmatched = await response.json();
                    const unmatchedDiv = document.getElementById('unmatched');
                    unmatchedDiv.innerHTML = '';
                    if (unmatched.length === 0) {
                        unmatchedDiv.innerHTML = '<div class="unmatched-entry"><span class="log-info">No unmatched SKUs</span></div>';
                    } else {
                        unmatched.slice(0, 50).forEach(item => {
                            const div = document.createElement('div');
                            div.className = 'unmatched-entry';
                            div.innerHTML = \`
                                <div class="unmatched-sku">❌ \${item.sku}</div>
                                <div class="change-title">\${item.title || item.sku}</div>
                                <div style="color:#555;font-size:9px;">Normalized: \${item.normalized}</div>
                                <div style="color:#FFAA00;font-size:9px;">Reason: \${item.reason}</div>
                            \`;
                            unmatchedDiv.appendChild(div);
                        });
                    }
                }
                
               async function clearAll() {
    // Just clear the dashboard displays locally
    document.getElementById('logs').innerHTML = '<div class="log-entry"><span class="log-info">> LOGS_CLEARED</span></div>';
    document.getElementById('changes').innerHTML = '<div class="change-entry"><span class="log-info">No changes yet</span></div>';
    document.getElementById('unmatched').innerHTML = '<div class="unmatched-entry"><span class="log-info">No unmatched SKUs</span></div>';
    
    // Also clear the local arrays in memory (frontend only)
    // No fetch calls to backend needed
    console.log('Dashboard cleared');
}
                async function refreshHealth() {
                    const response = await fetch('/health');
                    const health = await response.json();
                    document.getElementById('uptime').innerHTML = \`UPTIME: \${health.uptime}\`;
                }
                
                connectSSE();
                loadPriceChanges();
                loadUnmatched();
                refreshHealth();
                setInterval(loadPriceChanges, 10000);
                setInterval(loadUnmatched, 10000);
                setInterval(refreshHealth, 30000);
            </script>
        </body>
        </html>
    `);
});

// ================= CLEAR ENDPOINTS =================

app.post("/clear-logs", (req, res) => {
    syncLogs = [];
    res.json({ success: true });
});

app.post("/clear-changes", (req, res) => {
    priceChanges = [];
    unmatchedSkus = [];
    res.json({ success: true });
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
            body: JSON.stringify({ 
                client_id: CLIENT_ID, 
                client_secret: CLIENT_SECRET, 
                code 
            }),
        });
        const data = await response.json();
        console.log("FULL SHOPIFY RESPONSE:");
console.log(JSON.stringify(data, null, 2));
console.log("expires_in:", data.expires_in);
console.log("refresh_token:", data.refresh_token);
        
        if (data.access_token) {
            console.log("====================================");
console.log("COPY THIS TOKEN:");
console.log(data.access_token);
console.log("====================================");
            loadSyncState();
            
            const expiryDate = new Date(Date.now() + (data.expires_in || 86400) * 1000);
            addLog(`✅ Token valid until: ${expiryDate.toLocaleString()}`, "success");
            
            // Register webhook after auth
            registerWebhook();
            
            res.send(`
                <!DOCTYPE html>
                <html>
                <head><title>Auth Success</title></head>
                <body style="background:#0C0C0C; color:#00FF41; font-family: monospace; text-align: center; padding: 50px;">
                    <h2>✅ AUTHENTICATION_SUCCESSFUL</h2>
                    <p>Token saved. Expires: ${expiryDate.toLocaleString()}</p>
                    <p>Redirecting to dashboard...</p>
                    <script>setTimeout(() => { window.location.href = '/'; }, 2000);</script>
                </body>
                </html>
            `);
        } else {
            res.status(400).send(`❌ Auth Failed: ${JSON.stringify(data)}`);
        }
    } catch (err) {
        res.status(500).send(`❌ Auth Error: ${err.message}`);
    }
});
// ================= AUTO-START WITH EFFICIENT POLLING =================

loadSyncState();
loadLastPollTime();

setTimeout(async () => {
    const isValid = await ensureValidToken();
    if (isValid) {
        addLog("🔐 TOKEN_VALID - Starting EFFICIENT SYNC MODE", "success");
        addLog("   🔄 FULL SYNC: Available via button (manual)", "info");
        addLog("   ⚡ AUTO-POLL: Every one hour (only changed products)", "info");
        addLog("   💰 Detects price changes automatically", "info");
        addLog("   🆕 Detects new products automatically", "info");
        
        // Start efficient polling (only changed products)
        startEfficientPolling();
        // Start manual price monitoring (reverts manual changes)
startManualPriceMonitoring();
        
        // Run initial sync to establish baseline
        addLog("📡 Running initial sync to establish baseline...", "info");
        setTimeout(() => {
            runSync().catch(console.error);
        }, 5000);
        
    } else {
        addLog("🔑 NO_VALID_TOKEN - Visit /auth to authenticate", "warning");
    }
}, 2000);

// Remove the old cron - we use efficient polling instead
// No need for 15-minute cron anymore (polling handles it)

// Keep token refresh check (once daily)
cron.schedule("0 0 * * *", async () => {
    if (tokenData.refresh_token && tokenData.expires_at) {
        const daysUntilExpiry = (tokenData.expires_at - Date.now()) / (1000 * 60 * 60 * 24);
        if (daysUntilExpiry < 2) {
            addLog(`⚠️ Token expires in ${daysUntilExpiry.toFixed(1)} days. Attempting refresh...`, "warning");
            await refreshAccessToken();
        }
    }
});
// ================= START SERVER =================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log("\n" + "=".repeat(60));
    console.log("  ╔════════════════════════════════════════════════╗");
    console.log("  ║     OMS TO SHOPIFY PRICE SYNC                  ║");
    console.log("  ║     http://localhost:" + PORT + "                    ║");
    console.log("  ╚════════════════════════════════════════════════╝");
    console.log("=".repeat(60));
    console.log("\n  ⚡ OPTIMIZATIONS:");
    console.log("     30 concurrent requests");
    console.log("     100 products per batch");
    console.log("     10ms delay between batches");
    console.log("\n  📊 FEATURES:");
    console.log("     - Auto price sync every 6 hours");
    console.log("     - New product detection");
    console.log("     - Unmatched SKU tracking");
    console.log("     - Token auto-refresh");
    console.log("     - Real-time dashboard\n");
    console.log("  Press Ctrl+C to stop the server.\n");
});
