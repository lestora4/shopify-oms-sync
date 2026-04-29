const express = require("express");
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
const fs = require('fs');

const app = express();
app.use(express.json());

// ================= CONFIG =================

// 🔑 Shopify OAuth Credentials
const CLIENT_ID = "16268ecdaf07c5ebef41d27e9f78c7db";
const CLIENT_SECRET = "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";
const SHOP = "mello-distribution-test.myshopify.com";

// 🏷️ Price Lists
const SILVER_PRICE_LIST = "gid://shopify/PriceList/32255705394";
const GOLD_PRICE_LIST = "gid://shopify/PriceList/32255738162";
const PLATINUM_PRICE_LIST = "gid://shopify/PriceList/32255770930";

// 🔑 OMS
const OMS_API_URL = "https://api.omspavo.com/products";
const OMS_API_KEY = "1ae0b8a9-520b-28c2-737b-ec5f52198036";

const BATCH_SIZE = 50;
const CACHE_FILE = './price-cache.json';

// ================= PERSISTENT CACHE =================

function loadCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const data = fs.readFileSync(CACHE_FILE, 'utf8');
      const cached = JSON.parse(data);
      console.log(`📦 Loaded cache: ${Object.keys(cached.prices || {}).length} products`);
      return cached;
    }
  } catch (err) {
    console.log("⚠️ No cache file found, starting fresh");
  }
  return { timestamp: null, prices: {} };
}

function saveCache(cache) {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
    console.log(`💾 Saved cache: ${Object.keys(cache.prices).length} products`);
  } catch (err) {
    console.error("❌ Failed to save cache:", err.message);
  }
}

// Initialize cache
let lastSyncData = loadCache();

// ================= OAUTH TOKEN MANAGEMENT =================

let token = null;
let tokenExpiry = 0;

async function getAccessToken() {
  if (token && Date.now() < tokenExpiry) {
    return token;
  }

  console.log("🔄 Fetching new Shopify access token...");

  const url = `https://${SHOP}/admin/oauth/access_token`;
  const params = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET
  });

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params
    });

    const data = await response.json();

    if (!data.access_token) {
      throw new Error(`Token error: ${JSON.stringify(data)}`);
    }

    token = data.access_token;
    tokenExpiry = Date.now() + (data.expires_in - 300) * 1000;

    console.log("✅ Access token obtained successfully");
    return token;
  } catch (error) {
    console.error("❌ Failed to get access token:", error.message);
    throw error;
  }
}

// ================= SKU NORMALIZER =================
function normalizeSku(sku) {
  if (!sku) return "";
  return sku
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_box$/g, "")
    .replace(/-/g, "")
    .replace(/_/g, "");
}

// ================= GET ALL SHOPIFY VARIANTS =================

async function getAllShopifyVariants() {
  const t = await getAccessToken();

  let hasNextPage = true;
  let cursor = null;
  let all = [];

  while (hasNextPage) {
    const query = `
    query ($cursor: String) {
      productVariants(first: 250, after: $cursor) {
        edges {
          cursor
          node {
            id
            sku
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }`;

    const res = await fetch(`https://${SHOP}/admin/api/2024-10/graphql.json`, {
      method: "POST",
      headers: {
        "X-Shopify-Access-Token": t,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ query, variables: { cursor } })
    });

    const json = await res.json();
    const edges = json.data.productVariants.edges;

    edges.forEach(e => {
      if (e.node.sku) {
        all.push({
          id: e.node.id,
          sku: e.node.sku
        });
      }
    });

    hasNextPage = json.data.productVariants.pageInfo.hasNextPage;

    if (hasNextPage) {
      cursor = edges[edges.length - 1].cursor;
    }
  }

  console.log(`📦 Shopify Variants Loaded: ${all.length}`);
  return all;
}

// ================= GET VARIANT ID BY SKU =================

async function getVariantId(sku) {
  const t = await getAccessToken();
  
  const query = `
  query ($sku: String!) {
    productVariants(first: 1, query: $sku) {
      edges {
        node {
          id
        }
      }
    }
  }`;
  
  const res = await fetch(`https://${SHOP}/admin/api/2024-10/graphql.json`, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": t,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ 
      query, 
      variables: { sku: `sku:${sku}` } 
    })
  });
  
  const json = await res.json();
  const edges = json.data.productVariants.edges;
  
  return edges.length > 0 ? edges[0].node.id : null;
}

// ================= UPDATE SINGLE PRICE =================

async function updatePrice(priceListId, variantId, price) {
  const t = await getAccessToken();

  const mutation = `
  mutation {
    priceListFixedPricesAdd(
      priceListId: "${priceListId}",
      prices: [{
        variantId: "${variantId}",
        price: { amount: "${price}", currencyCode: USD }
      }]
    ) {
      userErrors { message }
    }
  }`;

  const res = await fetch(`https://${SHOP}/admin/api/2024-10/graphql.json`, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": t,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query: mutation })
  });

  const json = await res.json();

  if (json?.data?.priceListFixedPricesAdd?.userErrors?.length > 0) {
    console.log("❌ Price Error:", json.data.priceListFixedPricesAdd.userErrors);
  } else {
    console.log(`✅ B2B Catalog updated → ${variantId} = $${price}`);
  }
}

// ================= BULK UPDATE PRICES =================

async function updatePricesBulk(priceListId, updates, tierName) {
  if (updates.length === 0) return 0;
  
  const t = await getAccessToken();
  let updatedCount = 0;
  
  console.log(`   💰 Updating ${updates.length} prices for ${tierName}...`);
  
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    
    const mutation = `
      mutation priceListFixedPricesAdd($input: PriceListFixedPricesAddInput!) {
        priceListFixedPricesAdd(input: $input) {
          userErrors { field message }
        }
      }
    `;
    
    const variables = {
      input: {
        priceListId: priceListId,
        prices: batch.map(u => ({
          variantId: u.variantId,
          price: { amount: u.price.toString(), currencyCode: "USD" }
        }))
      }
    };
    
    try {
      const res = await fetch(`https://${SHOP}/admin/api/2024-10/graphql.json`, {
        method: "POST",
        headers: {
          "X-Shopify-Access-Token": t,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ query: mutation, variables })
      });
      
      const json = await res.json();
      
      if (json?.data?.priceListFixedPricesAdd?.userErrors?.length > 0) {
        console.log(`   ⚠️ Batch error:`, json.data.priceListFixedPricesAdd.userErrors[0].message);
      } else {
        updatedCount += batch.length;
        if (i % (BATCH_SIZE * 5) === 0 || i + BATCH_SIZE >= updates.length) {
          console.log(`   📦 ${tierName}: ${updatedCount}/${updates.length} updated`);
        }
      }
    } catch (err) {
      console.error(`   ❌ Batch failed:`, err.message);
    }
    
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  console.log(`   ✅ ${tierName}: ${updatedCount} prices updated`);
  return updatedCount;
}

// ================= FETCH OMS =================

async function fetchOMS() {
  console.log("📦 Fetching ALL OMS products...");
  
  let allProducts = [];
  let offset = 0;
  let total = null;
  let pagesFetched = 0;
  const limit = 100;
  
  while (true) {
    const url = `${OMS_API_URL}?limit=${limit}&offset=${offset}`;
    
    try {
      const res = await fetch(url, {
        headers: { "X-Pavo-Token": OMS_API_KEY }
      });
      
      const data = await res.json();
      pagesFetched++;
      
      if (total === null && data.total) {
        total = data.total;
        console.log(`   Total products in OMS: ${total}`);
      }
      
      if (!data.products || data.products.length === 0) {
        break;
      }
      
      allProducts.push(...data.products);
      console.log(`   Page ${pagesFetched}: +${data.products.length} (Total: ${allProducts.length}/${total})`);
      
      if (total && allProducts.length >= total) {
        console.log(`   ✅ Fetched all ${total} products`);
        break;
      }
      
      offset += limit;
      await new Promise(resolve => setTimeout(resolve, 50));
      
    } catch (err) {
      console.error(`   Error:`, err.message);
      break;
    }
  }
  
  console.log(`✅ Fetched ${allProducts.length} products in ${pagesFetched} pages`);
  
  // Transform OMS data
  const transformedProducts = [];
  
  for (let product of allProducts) {
    let itemNum = product['ITEM NO'] || product.item_num || product.sku;
    if (!itemNum) continue;
    
    let silver = 0, gold = 0, platinum = 0;
    
    if (product['Silver'] !== undefined) silver = parseFloat(product['Silver']) || 0;
    else if (product.silver !== undefined) silver = parseFloat(product.silver) || 0;
    
    if (product['Gold'] !== undefined) gold = parseFloat(product['Gold']) || 0;
    else if (product.gold !== undefined) gold = parseFloat(product.gold) || 0;
    
    if (product['Platimum'] !== undefined) platinum = parseFloat(product['Platimum']) || 0;
    else if (product.platinum !== undefined) platinum = parseFloat(product.platinum) || 0;
    
    if (product.prices && product.prices.business_type) {
      for (let p of product.prices.business_type) {
        if (p.type === "Silver") silver = p.value;
        if (p.type === "Gold") gold = p.value;
        if (p.type === "Platimum") platinum = p.value;
      }
    }
    
    transformedProducts.push({
      item_num: itemNum,
      prices: {
        business_type: [
          { type: "Silver", value: silver },
          { type: "Gold", value: gold },
          { type: "Platimum", value: platinum }
        ]
      }
    });
  }
  
  return transformedProducts;
}

// ================= OPTIMIZED SYNC WITH CHANGE DETECTION =================

async function syncAll() {
  const startTime = Date.now();
  console.log("\n🚀 SYNC CHECK STARTED...");
  console.log(`⏰ Time: ${new Date().toLocaleString()}\n`);

  let matched = 0;
  let notInOMS = 0;
  let pricesChanged = 0;
  let newProducts = 0;

  try {
    const omsProducts = await fetchOMS();
    const shopifyVariants = await getAllShopifyVariants();
    const totalVariants = shopifyVariants.length;

    // Build OMS Map
    let omsMap = {};

    for (let product of omsProducts) {
      if (!product || !product.item_num) continue;
      
      let silver = 0, gold = 0, platinum = 0;
      
      if (product.prices && product.prices.business_type) {
        for (let p of product.prices.business_type) {
          if (p.type === "Silver") silver = p.value;
          if (p.type === "Gold") gold = p.value;
          if (p.type === "Platimum") platinum = p.value;
        }
      }
      
      const normalizedSku = normalizeSku(product.item_num);
      omsMap[normalizedSku] = { silver, gold, platinum };
    }

    console.log(`📊 Statistics:`);
    console.log(`   OMS SKUs: ${Object.keys(omsMap).length}`);
    console.log(`   Shopify variants: ${totalVariants}`);
    console.log(`   Cached products: ${Object.keys(lastSyncData.prices).length}`);
    
    // Prepare updates
    let silverUpdates = [];
    let goldUpdates = [];
    let platinumUpdates = [];
    let unmatchedSkus = [];
    let newProductSkus = [];
    
    console.log(`\n🔄 Checking for changes...`);
    
    for (let i = 0; i < shopifyVariants.length; i++) {
      const v = shopifyVariants[i];
      
      if ((i + 1) % 500 === 0 || i + 1 === totalVariants) {
        const percent = (((i + 1) / totalVariants) * 100).toFixed(1);
        console.log(`   Progress: ${i + 1}/${totalVariants} (${percent}%) - Changes: ${pricesChanged}, New: ${newProducts}`);
      }
      
      const normalizedSku = normalizeSku(v.sku);
      const oms = omsMap[normalizedSku];
      
      if (!oms) {
        notInOMS++;
        if (unmatchedSkus.length < 20) {
          unmatchedSkus.push(v.sku);
        }
        continue;
      }
      
      matched++;
      
      // Check if product exists in cache
      const cached = lastSyncData.prices[v.id];
      const isNewProduct = !cached;
      
      if (isNewProduct) {
        newProducts++;
        if (newProductSkus.length < 20) {
          newProductSkus.push(v.sku);
        }
      }
      
      // Check price changes
      const silverChanged = !cached || cached.silver !== oms.silver;
      const goldChanged = !cached || cached.gold !== oms.gold;
      const platinumChanged = !cached || cached.platinum !== oms.platinum;
      
      if (silverChanged || goldChanged || platinumChanged) {
        pricesChanged++;
        
        if (silverChanged) silverUpdates.push({ variantId: v.id, price: oms.silver });
        if (goldChanged) goldUpdates.push({ variantId: v.id, price: oms.gold });
        if (platinumChanged) platinumUpdates.push({ variantId: v.id, price: oms.platinum });
        
        // Update cache
        lastSyncData.prices[v.id] = { 
          silver: oms.silver, 
          gold: oms.gold, 
          platinum: oms.platinum 
        };
      }
    }
    
    if (unmatchedSkus.length > 0) {
      console.log(`\n📋 Unmatched Shopify SKUs (${unmatchedSkus.length} total):`);
      unmatchedSkus.slice(0, 10).forEach(sku => {
        console.log(`   ❌ ${sku}`);
      });
    }
    
    if (newProductSkus.length > 0) {
      console.log(`\n🆕 New Products Detected (${newProducts}):`);
      newProductSkus.slice(0, 10).forEach(sku => {
        console.log(`   ✅ ${sku}`);
      });
    }
    
    console.log(`\n📊 Change Summary:`);
    console.log(`   Total matched: ${matched}`);
    console.log(`   New products: ${newProducts}`);
    console.log(`   Price changes: ${pricesChanged}`);
    console.log(`   Silver updates: ${silverUpdates.length}`);
    console.log(`   Gold updates: ${goldUpdates.length}`);
    console.log(`   Platinum updates: ${platinumUpdates.length}`);
    
    // Send updates ONLY if there are changes
    let totalUpdates = 0;
    
    if (silverUpdates.length > 0 || goldUpdates.length > 0 || platinumUpdates.length > 0) {
      console.log(`\n📤 Sending updates to Shopify...`);
      
      if (silverUpdates.length > 0) {
        totalUpdates += await updatePricesBulk(SILVER_PRICE_LIST, silverUpdates, "Silver");
      }
      if (goldUpdates.length > 0) {
        totalUpdates += await updatePricesBulk(GOLD_PRICE_LIST, goldUpdates, "Gold");
      }
      if (platinumUpdates.length > 0) {
        totalUpdates += await updatePricesBulk(PLATINUM_PRICE_LIST, platinumUpdates, "Platinum");
      }
    } else {
      console.log(`\n💡 No changes detected. Skipping updates.`);
    }
    
    // Update timestamp and save cache
    lastSyncData.timestamp = new Date();
    saveCache(lastSyncData);
    
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log("\n==================================");
    console.log("📊 FINAL REPORT");
    console.log(`✅ Processed: ${totalVariants} Shopify variants`);
    console.log(`✅ Matched with OMS: ${matched}`);
    console.log(`🆕 New products found: ${newProducts}`);
    console.log(`🔄 Price changes: ${pricesChanged}`);
    console.log(`✅ B2B Catalog updates: ${totalUpdates}`);
    console.log(`⚠️  Not in OMS: ${notInOMS}`);
    console.log(`⏱️  Total time: ${totalTime} seconds`);
    
    if (pricesChanged === 0 && newProducts === 0) {
      console.log(`💡 No changes - catalog prices are up to date!`);
    }
    console.log("==================================");
    console.log(`🎉 SYNC COMPLETED at ${new Date().toLocaleTimeString()}\n`);
    
    return { matched, newProducts, pricesChanged, totalUpdates, notInOMS };

  } catch (err) {
    console.error("❌ Sync error:", err.message);
    console.error(err.stack);
  }
}

// ================= DEBUG ENDPOINTS =================

app.get("/debug/missing", async (req, res) => {
  try {
    const omsProducts = await fetchOMS();
    const shopifyVariants = await getAllShopifyVariants();
    
    let omsMap = {};
    for (let product of omsProducts) {
      if (product.item_num) {
        omsMap[normalizeSku(product.item_num)] = true;
      }
    }
    
    let missing = [];
    for (let v of shopifyVariants) {
      if (!omsMap[normalizeSku(v.sku)]) {
        missing.push(v.sku);
      }
    }
    
    res.json({
      totalMissing: missing.length,
      missingSkus: missing
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/debug/status", (req, res) => {
  res.json({
    lastSync: lastSyncData.timestamp,
    cachedProducts: Object.keys(lastSyncData.prices).length,
    cacheFileExists: fs.existsSync(CACHE_FILE)
  });
});

// ================= WEBHOOKS =================

app.post("/webhook", async (req, res) => {
  console.log("📩 Webhook received - triggering sync");
  syncAll().catch(console.error);
  res.send("OK");
});

// ================= MANUAL SYNC ENDPOINT =================

app.post("/sync", async (req, res) => {
  const result = await syncAll();
  res.json({ status: "Sync completed", timestamp: new Date(), result });
});

// ================= HEALTH CHECK =================

app.get("/health", (req, res) => {
  res.json({
    status: "running",
    timestamp: new Date().toISOString(),
    tokenValid: token && Date.now() < tokenExpiry,
    lastSync: lastSyncData.timestamp,
    cachedProducts: Object.keys(lastSyncData.prices).length
  });
});

// ================= TOKEN REFRESH ENDPOINT =================

app.post("/refresh-token", async (req, res) => {
  try {
    token = null;
    tokenExpiry = 0;
    await getAccessToken();
    res.json({ success: true, message: "Token refreshed" });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ================= START SERVER =================

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`\n🔥 Optimized Server running on port ${PORT}`);
  console.log("📋 Configuration:");
  console.log(`   - Shopify: ${SHOP}`);
  console.log(`   - B2B Catalogs: Silver, Gold, Platinum`);
  console.log(`   - SKU Normalization: ON`);
  console.log(`   - Change Detection: ON (with persistence)`);
  console.log(`   - Cache File: ${CACHE_FILE}`);
  console.log("\n🔄 Auto-sync every 30 minutes");
  console.log(`📨 Webhook: POST to http://localhost:${PORT}/webhook`);
  console.log(`🔧 Manual sync: POST to http://localhost:${PORT}/sync`);
  console.log(`🔍 Debug: http://localhost:${PORT}/debug/status\n`);
  
  syncAll();
});

setInterval(syncAll, 30 * 60 * 1000);