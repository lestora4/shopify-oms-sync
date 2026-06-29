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
const REDIRECT_URI = process.env.REDIRECT_URI || "http://localhost:3000/auth/callback";
const TOKEN_FILE = "./token.json";
const SYNC_STATE_FILE = "./sync_state.json";

const SILVER_PRICE_LIST = "gid://shopify/PriceList/32259899698";
const GOLD_PRICE_LIST = "gid://shopify/PriceList/32259866930";
const PLATINUM_PRICE_LIST = "gid://shopify/PriceList/32259834162";

const OMS_API_KEY = "1ae0b8a9-520b-28c2-737b-ec5f52198036";

// ================= PERFORMANCE CONFIG =================
const CONCURRENT_REQUESTS = 30;
const BATCH_UPDATE_SIZE = 100;
const REQUEST_DELAY_MS = 10;

// ================= TOKEN MANAGEMENT =================
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

// ================= FIXED getPrice =================

function getPrice(prices, type) {
    if (!prices || !prices.business_type) return 0;
    
    const searchType = type.toLowerCase();
    const found = prices.business_type.find(p => {
        if (!p.type) return false;
        const pType = p.type.toLowerCase();
        
        if (searchType === "platinum") {
            return pType === "platinum" || pType === "platimum";
        }
        return pType === searchType;
    });
    
    return found ? Number(found.value) : 0;
}

// ================= FIXED lookupSkuInOMS (ONLY ONE VERSION) =================

async function lookupSkuInOMS(sku) {
    if (!sku) return null;
    
    try {
        const url = `https://api.omspavo.com/product/variants?item_num=${encodeURIComponent(sku)}`;
        const response = await fetch(url, {
            headers: { "X-Pavo-Token": OMS_API_KEY }
        });
        
        if (!response.ok) return null;
        
        const data = await response.json();
        if (!data.variants || data.variants.length === 0) return null;
        
        // Find the best variant (piece > each > other)
        let selectedVariant = data.variants.find(v => 
            v.unit?.type?.toLowerCase() === "piece"
        ) || data.variants.find(v => 
            v.unit?.type?.toLowerCase() === "each"
        ) || data.variants.find(v => 
            !v.sku?.toLowerCase().includes("_box") && 
            !v.sku?.toLowerCase().includes("_case")
        ) || data.variants[0];
        
        if (!selectedVariant) return null;
        
        // Extract prices using getPrice function
        const silver = getPrice(selectedVariant.prices, "Silver");
        const gold = getPrice(selectedVariant.prices, "Gold");
        const platinum = getPrice(selectedVariant.prices, "Platinum");
        
        // Log success for AURA-SPK-BK
        if (sku === "AURA-SPK-BK") {
            addLog(`✅ AURA-SPK-BK found: S:$${silver} G:$${gold} P:$${platinum}`, "success");
        }
        
        return {
            silver: silver,
            gold: gold,
            platinum: platinum,
            originalSku: selectedVariant.sku,
            unit: selectedVariant.unit?.type
        };
        
    } catch (err) {
        if (sku === "AURA-SPK-BK") {
            addLog(`❌ Error looking up AURA-SPK-BK: ${err.message}`, "error");
        }
        return null;
    }
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

// ================= TOKEN HELPERS =================

function getToken() {
    const envToken = process.env.SHOPIFY_ACCESS_TOKEN;
    if (envToken) {
        return envToken;
    }
    
    if (fs.existsSync(TOKEN_FILE)) {
        try {
            const data = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
            if (data.access_token) {
                return data.access_token;
            }
        } catch (err) {}
    }
    
    return null;
}

function saveToken(accessToken, expiresIn = null, refreshToken = null) {
    tokenData = {
        access_token: accessToken,
        expires_at: null,
        refresh_token: refreshToken || null
    };
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokenData));
    addLog("✅ Token saved successfully", "success");
}

async function ensureValidToken() {
    const token = getToken();
    if (!token) {
        addLog("❌ No valid token. Please set SHOPIFY_ACCESS_TOKEN environment variable or visit /auth", "error");
        return false;
    }
    addLog("✅ Token found and valid", "success");
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

// ================= GET CURRENT PRICES =================

async function getCurrentPriceListPrices(variantId) {
    try {
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

// ================= EFFICIENT OMS POLLING =================

let lastOmsPollTime = null;

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

function startEfficientPolling() {
    loadLastPollTime();
    
    setInterval(async () => {
        if (!isRunning) {
            addLog("🔄 Auto-poll: Running full comparison sync...", "info");
            await runSync();
        }
    }, 7200000);
    
    addLog("🔔 Started efficient OMS polling (every 2 hours)", "success");
}

// ================= MAIN SYNC FUNCTION =================

async function runSync() {
    if (isRunning) {
        addLog("⚠️ Sync already in progress, skipping...", "warning");
        return;
    }
    
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
            currentSync.status = "completed";
            isRunning = false;
            return;
        }
        
        addLog(`\n💰 Step 3: Checking current Shopify prices for ${foundInOMS.length} products...`, "info");
        
        const priceCheckResults = await processConcurrently(
            foundInOMS,
            CONCURRENT_REQUESTS,
            async (item) => {
                const currentPrices = await getCurrentPriceListPrices(item.id);
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
            currentSync.status = "completed";
            isRunning = false;
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
                    addLog(`   ⚠️ Batch ${batchNum} had errors`, "warning");
                }
            } catch (err) {
                errorCount += batch.length;
                addLog(`   ❌ Batch ${batchNum} failed: ${err.message}`, "error");
            }
            
            if (i + BATCH_UPDATE_SIZE < updates.length) {
                await new Promise(r => setTimeout(r, 100));
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
        currentSync.errors = (currentSync.errors || 0) + 1;
    } finally {
        isRunning = false;
        currentSync.status = "completed";
        currentSync.endTime = Date.now();
    }
}

// ================= WEBHOOKS AND OTHER ENDPOINTS =================
// ... (keep all your existing webhook and endpoint code here)

// ================= AUTO-START =================

loadSyncState();
loadLastPollTime();

setTimeout(async () => {
    const isValid = await ensureValidToken();
    if (isValid) {
        addLog("🔐 TOKEN_VALID - Starting EFFICIENT SYNC MODE", "success");
        addLog("   🔄 FULL SYNC: Available via button (manual)", "info");
        addLog("   ⚡ AUTO-POLL: Every 2 hours (only changed products)", "info");
        addLog("   💰 Detects price changes automatically", "info");
        addLog("   🆕 Detects new products automatically", "info");
        
        startEfficientPolling();
        startManualPriceMonitoring();
        
        addLog("📡 Running initial sync to establish baseline...", "info");
        setTimeout(() => {
            runSync().catch(console.error);
        }, 5000);
        
    } else {
        addLog("🔑 NO_VALID_TOKEN - Visit /auth to authenticate", "warning");
    }
}, 2000);

// ================= START SERVER =================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log("\n" + "=".repeat(60));
    console.log("  ╔════════════════════════════════════════════════╗");
    console.log("  ║     OMS TO SHOPIFY PRICE SYNC                  ║");
    console.log("  ║     Server running on port " + PORT + "                    ║");
    console.log("  ╚════════════════════════════════════════════════╝");
    console.log("=".repeat(60));
    console.log("\n  ⚡ OPTIMIZATIONS:");
    console.log("     30 concurrent requests");
    console.log("     100 products per batch");
    console.log("     10ms delay between batches");
    console.log("\n  📊 FEATURES:");
    console.log("     - Auto price sync every 2 hours");
    console.log("     - New product detection");
    console.log("     - Unmatched SKU tracking");
    console.log("     - Real-time dashboard\n");
});
