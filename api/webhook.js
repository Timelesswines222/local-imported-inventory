export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).send("Method Not Allowed");
    }

    const topic = req.headers["x-shopify-topic"];
    console.log("🔥 Webhook hit, topic:", topic);

    const data = req.body;

    const SHOP = process.env.SHOPIFY_STORE;
    const TOKEN = process.env.SHOPIFY_ADMIN_API_TOKEN;
    const LOCAL = process.env.LOCATION_LOCAL;
    const IMPORTED = process.env.LOCATION_IMPORTED;
    const SPLIT_PLAN_ATTR = "inventory_split_plan_v1";

    function isAuthErrorMessage(value) {
      if (!value) return false;
      const text = String(value).toLowerCase();
      return (
        text.includes("invalid api key") ||
        text.includes("access token") ||
        text.includes("wrong password") ||
        text.includes("unrecognized login")
      );
    }

    function hasAuthError(errors) {
      if (!errors) return false;
      if (Array.isArray(errors)) return errors.some((entry) => hasAuthError(entry));
      if (typeof errors === "object") return Object.values(errors).some((entry) => hasAuthError(entry));
      return isAuthErrorMessage(errors);
    }

    function getShopifyFailureReason(response) {
      if (!response || !response.headers) return "";
      return String(
        response.headers.get("x-shopify-api-request-failure-reason") ||
        response.headers.get("X-Shopify-API-Request-Failure-Reaason") ||
        ""
      ).toLowerCase();
    }

    function isLikelyAuthOrScopeFailure(response, json) {
      const status = Number(response?.status || 0);
      const failureReason = getShopifyFailureReason(response);
      if (status === 401 || status === 403) return true;
      if (failureReason.includes("invalid_api_key") || failureReason.includes("unauthorized")) return true;
      const payloadErrors = json?.errors ?? json?.error ?? json;
      return hasAuthError(payloadErrors);
    }

    function buildAdminApiError(action, response, json) {
      const status = Number(response?.status || 0);
      const payloadErrors = json?.errors ?? json?.error ?? null;
      const details = payloadErrors ? ` details=${JSON.stringify(payloadErrors)}` : "";
      return new Error(`Shopify Admin API ${action} failed (status ${status}).${details}`);
    }

    function assertConfig() {
      if (!SHOP || !TOKEN || !LOCAL || !IMPORTED) {
        throw new Error("Missing required env vars: SHOPIFY_STORE, SHOPIFY_ADMIN_API_TOKEN, LOCATION_LOCAL, LOCATION_IMPORTED");
      }
    }

    assertConfig();

    async function shopifyAdminGraphql(body) {
      const response = await fetch(`https://${SHOP}/admin/api/2026-04/graphql.json`, {
        method: "POST",
        headers: {
          "X-Shopify-Access-Token": TOKEN,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });
      const json = await response.json();
      if (isLikelyAuthOrScopeFailure(response, json)) {
        throw new Error("Shopify Admin API authentication/scope check failed (GraphQL).");
      }
      if (json.errors) {
        console.log("❌ GraphQL errors:", JSON.stringify(json.errors));
      }
      return { response, json };
    }

    /**
     * True when checkout looks like ship-to-address (UPS/carrier), not retail/local pickup.
     * Used to prefer imported-first splits and to try merging pickup-labeled FOs into shipping FOs.
     */
    function orderLooksShippedToAddress(orderPayload) {
      const attrs = orderPayload.note_attributes || [];
      for (const a of attrs) {
        const name = String(a?.name || "").toLowerCase();
        const val = String(a?.value || "").toLowerCase();
        if (
          name.includes("shipping method") ||
          name === "selected shipping method" ||
          name.includes("delivery method")
        ) {
          if (!val) continue;
          if (val.includes("pickup") || val.includes("pick up") || val.includes("collect")) {
            return false;
          }
          if (
            val.includes("ups") ||
            val.includes("fedex") ||
            val.includes("usps") ||
            val.includes("dhl") ||
            val.includes("ship")
          ) {
            return true;
          }
        }
      }
      for (const sl of orderPayload.shipping_lines || []) {
        const t = String(sl.title || "").toLowerCase();
        if (!t) continue;
        if (
          t.includes("pickup") ||
          t.includes("pick up") ||
          t.includes("in store") ||
          t.includes("in-store") ||
          t.includes("local delivery")
        ) {
          return false;
        }
        if (
          t.includes("ups") ||
          t.includes("fedex") ||
          t.includes("usps") ||
          t.includes("dhl") ||
          t.includes("standard") ||
          t.includes("express") ||
          t.includes("ground") ||
          t.includes("shipping") ||
          t.includes("delivery")
        ) {
          return true;
        }
      }
      return false;
    }

    function deliveryMethodIsPickup(dm) {
      if (!dm) return false;
      const type = String(dm.methodType || "");
      if (type === "PICK_UP" || type === "PICKUP_POINT") return true;
      const label = String(dm.presentedName || "").toLowerCase();
      return label.includes("pickup") || label.includes("pick up") || label.includes("in store");
    }

    function getEffectiveRoutingMode(orderPayload) {
      if (orderLooksShippedToAddress(orderPayload)) {
        return "imported_first";
      }
      return String(process.env.INVENTORY_SPLIT_ROUTING || "local_first").toLowerCase() ===
        "imported_first"
        ? "imported_first"
        : "local_first";
    }

    async function fetchFulfillmentOrdersGraphql(orderId) {
      const orderGid = `gid://shopify/Order/${orderId}`;
      const { json } = await shopifyAdminGraphql({
        query: `
          query ($orderId: ID!) {
            order(id: $orderId) {
              fulfillmentOrders(first: 30) {
                nodes {
                  id
                  status
                  deliveryMethod {
                    methodType
                    presentedName
                  }
                  assignedLocation {
                    location {
                      legacyResourceId
                      name
                    }
                  }
                }
              }
            }
          }
        `,
        variables: { orderId: orderGid },
      });
      if (!json?.data?.order) {
        console.log("⏭ GraphQL order not found or missing fulfillmentOrders for:", orderId);
      }
      return json?.data?.order?.fulfillmentOrders?.nodes || [];
    }

    async function fulfillmentOrderIdsMergeableWith(fulfillmentOrderGid) {
      const { json } = await shopifyAdminGraphql({
        query: `
          query ($id: ID!) {
            fulfillmentOrder(id: $id) {
              fulfillmentOrdersForMerge(first: 20) {
                nodes {
                  id
                }
              }
            }
          }
        `,
        variables: { id: fulfillmentOrderGid },
      });
      const nodes = json?.data?.fulfillmentOrder?.fulfillmentOrdersForMerge?.nodes || [];
      return new Set(nodes.map((n) => n.id));
    }

    async function mergeTwoFulfillmentOrders(foGidA, foGidB) {
      const runOnce = async (first, second) => {
        const { json } = await shopifyAdminGraphql({
          query: `
            mutation ($inputs: [FulfillmentOrderMergeInput!]!) {
              fulfillmentOrderMerge(fulfillmentOrderMergeInputs: $inputs) {
                fulfillmentOrderMerges {
                  fulfillmentOrder {
                    id
                    status
                    deliveryMethod {
                      methodType
                      presentedName
                    }
                  }
                }
                userErrors {
                  field
                  message
                  code
                }
              }
            }
          `,
          variables: {
            inputs: [
              {
                mergeIntents: [
                  { fulfillmentOrderId: first },
                  { fulfillmentOrderId: second },
                ],
              },
            ],
          },
        });
        const payload = json?.data?.fulfillmentOrderMerge;
        const userErrors = payload?.userErrors || [];
        if (userErrors.length) {
          console.log("⚠️ fulfillmentOrderMerge userErrors:", JSON.stringify(userErrors));
          return false;
        }
        const merges = payload?.fulfillmentOrderMerges || [];
        if (merges.length) {
          console.log(
            "✅ Merged fulfillment orders:",
            JSON.stringify(merges.map((m) => m.fulfillmentOrder?.id))
          );
          return true;
        }
        return false;
      };

      if (await runOnce(foGidA, foGidB)) return true;
      if (await runOnce(foGidB, foGidA)) return true;
      return false;
    }

    /**
     * When Shopify marks a pickup-type FO as mergeable with a carrier-shipping FO, merge them so
     * the admin shows a single shipping method (e.g. UPS) instead of "In Store Pickup" on a split group.
     */
    async function tryMergePickupFulfillmentOrdersIntoShipping(orderId) {
      const enabled = String(process.env.TRY_MERGE_PICKUP_FULFILLMENT_ORDERS ?? "1") !== "0";
      if (!enabled || !orderLooksShippedToAddress(data)) return;

      const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

      try {
        for (let attempt = 0; attempt < 6; attempt++) {
          let nodes = await fetchFulfillmentOrdersGraphql(orderId);
          nodes = (nodes || []).filter((n) => {
            const st = String(n?.status || "").toUpperCase();
            return st === "OPEN" || st === "SCHEDULED" || st === "ON_HOLD";
          });
          const pickupNodes = nodes.filter((n) => deliveryMethodIsPickup(n.deliveryMethod));
          const shipNodes = nodes.filter((n) => !deliveryMethodIsPickup(n.deliveryMethod));
          if (!pickupNodes.length) {
            return;
          }
          if (!shipNodes.length) {
            if (attempt === 0) {
              console.log(
                "⏭ Pickup-type fulfillment orders present but no merge partner with a shipping delivery method."
              );
            }
            return;
          }

          const shipPreferImported = [...shipNodes].sort((a, b) => {
            const la = String(a?.assignedLocation?.location?.legacyResourceId || "");
            const lb = String(b?.assignedLocation?.location?.legacyResourceId || "");
            if (la === String(IMPORTED) && lb !== String(IMPORTED)) return -1;
            if (lb === String(IMPORTED) && la !== String(IMPORTED)) return 1;
            return 0;
          });

          let mergedAny = false;
          pickupLoop: for (const pickup of pickupNodes) {
            const pickupId = pickup.id;
            for (const ship of shipPreferImported) {
              const shipId = ship.id;
              if (pickupId === shipId) continue;

              const mergeableFromShip = await fulfillmentOrderIdsMergeableWith(shipId);
              const mergeableFromPickup = await fulfillmentOrderIdsMergeableWith(pickupId);
              const canMerge =
                mergeableFromShip.has(pickupId) ||
                mergeableFromPickup.has(shipId);

              if (!canMerge) continue;

              const ok = await mergeTwoFulfillmentOrders(shipId, pickupId);
              if (ok) {
                mergedAny = true;
                await sleep(400);
                break pickupLoop;
              }
            }
          }

          if (!mergedAny) return;
        }
      } catch (err) {
        console.log("⚠️ tryMergePickupFulfillmentOrdersIntoShipping:", err.message);
      }
    }

    // 🔥 GraphQL: Get inventory_item_id
    async function getInventoryItemId(variantId) {
      try {
        const query = {
          query: `
            query ($id: ID!) {
              productVariant(id: $id) {
                inventoryItem {
                  id
                }
              }
            }
          `,
          variables: {
            id: `gid://shopify/ProductVariant/${variantId}`
          }
        };

        const response = await fetch(
          `https://${SHOP}/admin/api/2026-04/graphql.json`,
          {
            method: "POST",
            headers: {
              "X-Shopify-Access-Token": TOKEN,
              "Content-Type": "application/json",
            },
            body: JSON.stringify(query),
          }
        );

        const json = await response.json();

        if (isLikelyAuthOrScopeFailure(response, json)) {
          throw new Error("Shopify Admin API authentication/scope check failed. Verify SHOPIFY_ADMIN_API_TOKEN and required app scopes.");
        }

        if (json.errors) {
          console.log("❌ GraphQL errors:", JSON.stringify(json.errors));
        }

        const inventoryItemGid =
          json?.data?.productVariant?.inventoryItem?.id;

        if (!inventoryItemGid) {
          console.log("⏭ No inventoryItem for variant:", variantId);
          return null;
        }

        return inventoryItemGid.split("/").pop();

      } catch (err) {
        console.log("⏭ GraphQL error for variant:", variantId, err.message);
        return null;
      }
    }

    // 🔹 Get inventory quantities by location
    async function getInventoryQuantities(inventoryItemId, locationId) {
      const query = {
        query: `
          query ($inventoryItemId: ID!, $locationId: ID!) {
            inventoryItem(id: $inventoryItemId) {
              inventoryLevel(locationId: $locationId) {
                quantities(names: ["on_hand", "available", "committed"]) {
                  name
                  quantity
                }
              }
            }
          }
        `,
        variables: {
          inventoryItemId: `gid://shopify/InventoryItem/${inventoryItemId}`,
          locationId: `gid://shopify/Location/${locationId}`,
        },
      };

      const response = await fetch(
        `https://${SHOP}/admin/api/2026-04/graphql.json`,
        {
          method: "POST",
          headers: {
            "X-Shopify-Access-Token": TOKEN,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(query),
        }
      );

      const json = await response.json();

      if (isLikelyAuthOrScopeFailure(response, json)) {
        throw new Error("Shopify Admin API authentication/scope check failed while reading inventory levels.");
      }
      if (!response.ok) {
        throw buildAdminApiError("read inventory levels", response, json);
      }

      const quantities =
        json?.data?.inventoryItem?.inventoryLevel?.quantities || [];

      const map = { on_hand: 0, available: 0, committed: 0 };
      for (const entry of quantities) {
        if (!entry?.name) continue;
        map[entry.name] = Number(entry.quantity || 0);
      }

      return map;
    }

    // 🔹 Adjust inventory
    async function adjustInventory(inventoryItemId, locationId, adjustment) {
      const response = await fetch(
        `https://${SHOP}/admin/api/2026-04/inventory_levels/adjust.json`,
        {
          method: "POST",
          headers: {
            "X-Shopify-Access-Token": TOKEN,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            location_id: locationId,
            inventory_item_id: inventoryItemId,
            available_adjustment: adjustment,
          }),
        }
      );

      const json = await response.json();
      if (isLikelyAuthOrScopeFailure(response, json)) {
        throw new Error("Shopify Admin API authentication/scope check failed while adjusting inventory.");
      }
      if (!response.ok) {
        throw buildAdminApiError("adjust inventory", response, json);
      }
      console.log("Adjust response:", JSON.stringify(json));
    }

    // 🔹 Get fulfillment orders
    async function getFulfillmentOrders(orderId) {
      const response = await fetch(
        `https://${SHOP}/admin/api/2026-04/orders/${orderId}/fulfillment_orders.json`,
        {
          headers: {
            "X-Shopify-Access-Token": TOKEN,
          },
        }
      );

      const json = await response.json();
      if (isLikelyAuthOrScopeFailure(response, json)) {
        throw new Error(
          "Shopify Admin API authentication/scope check failed while reading fulfillment orders. " +
          "Ensure token can access fulfillment orders (for example: read_assigned_fulfillment_orders and/or read_merchant_managed_fulfillment_orders)."
        );
      }
      if (!response.ok) {
        throw buildAdminApiError("read fulfillment orders", response, json);
      }
      return json.fulfillment_orders || [];
    }

    // 🔹 Move fulfillment line item to location
    async function moveFulfillmentLineItem(fulfillmentOrderId, lineItemId, quantity, locationId) {
      const response = await fetch(
        `https://${SHOP}/admin/api/2026-04/fulfillment_orders/${fulfillmentOrderId}/move.json`,
        {
          method: "POST",
          headers: {
            "X-Shopify-Access-Token": TOKEN,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            fulfillment_order: {
              new_location_id: locationId,
              fulfillment_order_line_items: [
                {
                  id: lineItemId,
                  quantity: quantity,
                }
              ]
            }
          }),
        }
      );

      const json = await response.json();
      if (isLikelyAuthOrScopeFailure(response, json)) {
        throw new Error("Shopify Admin API authentication/scope check failed while moving fulfillment line item.");
      }
      if (!response.ok) {
        throw buildAdminApiError("move fulfillment line item", response, json);
      }
      console.log("🚚 Move response:", JSON.stringify(json));
      return json;
    }

    /**
     * Remove inventory_split_plan_v1 from order note_attributes so it does not appear under
     * Additional details. Preserves all other note attributes.
     */
    async function removeSplitPlanNoteAttribute(orderId) {
      try {
        const orderResponse = await fetch(
          `https://${SHOP}/admin/api/2026-04/orders/${orderId}.json?fields=id,note_attributes`,
          {
            headers: {
              "X-Shopify-Access-Token": TOKEN,
            },
          }
        );
        const orderJson = await orderResponse.json();
        if (isLikelyAuthOrScopeFailure(orderResponse, orderJson)) {
          throw new Error("Shopify Admin API authentication/scope check failed while reading order note attributes.");
        }
        if (!orderResponse.ok) {
          throw buildAdminApiError("read order note attributes", orderResponse, orderJson);
        }
        const existingAttrs = orderJson?.order?.note_attributes || [];
        const attrsWithoutSplitPlan = existingAttrs.filter(
          (attr) => attr?.name !== SPLIT_PLAN_ATTR
        );
        if (attrsWithoutSplitPlan.length === existingAttrs.length) {
          return;
        }

        const response = await fetch(
          `https://${SHOP}/admin/api/2026-04/orders/${orderId}.json`,
          {
            method: "PUT",
            headers: {
              "X-Shopify-Access-Token": TOKEN,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              order: {
                id: orderId,
                note_attributes: attrsWithoutSplitPlan,
              },
            }),
          }
        );

        const json = await response.json();
        console.log("📝 Removed split plan from note_attributes, status:", response.status);

        if (isLikelyAuthOrScopeFailure(response, json)) {
          throw new Error("Shopify Admin API authentication/scope check failed while updating order note attributes.");
        }
        if (!response.ok) {
          throw buildAdminApiError("remove split plan note attribute", response, json);
        }

        if (json?.errors) {
          console.log("⚠️ Remove split plan errors:", JSON.stringify(json.errors));
        }
      } catch (err) {
        console.log("⚠️ Failed to remove split plan from note_attributes:", err.message);
      }
    }

    // 🔹 Read split plan from order note attributes
    function getSplitPlanFromNoteAttributes(noteAttributes) {
      const attrs = noteAttributes || [];
      const planAttr = attrs.find((attr) => attr?.name === SPLIT_PLAN_ATTR);

      if (!planAttr?.value) return null;

      try {
        return JSON.parse(planAttr.value);
      } catch (err) {
        console.log("⚠️ Invalid split plan JSON:", err.message);
        return null;
      }
    }

    /**
     * Imported first: assign up to IMPORTED capacity, remainder from LOCAL.
     * Preferring IMPORTED first often keeps the larger fulfillment group on the warehouse FO that
     * already has checkout shipping (e.g. UPS). Local-only remainder still follows that location's
     * delivery profile in Shopify admin (pickup vs carrier).
     */
    function computeImportedFirstSplit(orderedQty, localCapacity, importedCapacity) {
      const local = Math.max(0, Number(localCapacity) || 0);
      const imported = Math.max(0, Number(importedCapacity) || 0);
      const importedQty = Math.min(orderedQty, imported);
      const remaining = orderedQty - importedQty;
      const localQty = Math.min(remaining, local);
      return { localQty, importedQty };
    }

    /**
     * Local first: assign up to LOCAL capacity, remainder from IMPORTED.
     * Inventory is not adjusted via API — Shopify commits at checkout; we only move fulfillment lines.
     */
    function computeLocalFirstSplit(orderedQty, localCapacity, importedCapacity) {
      const local = Math.max(0, Number(localCapacity) || 0);
      const imported = Math.max(0, Number(importedCapacity) || 0);
      const localQty = Math.min(orderedQty, local);
      const remaining = orderedQty - localQty;
      const importedQty = Math.min(remaining, imported);
      return { localQty, importedQty };
    }

    /**
     * Build current order assignment map by variant from fulfillment orders.
     * This lets us reconstruct pre-order capacity as: available + assigned_for_this_order.
     */
    function buildAssignedQtyByVariant(fulfillmentOrders) {
      const assigned = {};
      for (const fo of fulfillmentOrders || []) {
        const loc = String(fo.assigned_location_id);
        for (const foLine of fo.line_items || []) {
          const variantId = String(foLine.variant_id || "");
          if (!variantId) continue;
          const q = Number(
            foLine.fulfillable_quantity ?? foLine.quantity ?? 0
          );
          if (q <= 0) continue;

          if (!assigned[variantId]) {
            assigned[variantId] = { local: 0, imported: 0 };
          }

          if (loc === String(LOCAL)) {
            assigned[variantId].local += q;
          } else if (loc === String(IMPORTED)) {
            assigned[variantId].imported += q;
          }
        }
      }
      return assigned;
    }

    async function getFulfillmentOrdersWithRetry(orderId, attempts = 5, delayMs = 600) {
      for (let i = 0; i < attempts; i++) {
        const fos = await getFulfillmentOrders(orderId);
        if (fos.length > 0) return fos;
        await new Promise((r) => setTimeout(r, delayMs));
      }
      return getFulfillmentOrders(orderId);
    }

    /**
     * Move fulfillment quantities so assigned locations match target localQty / importedQty for this variant.
     * For carrier-shipped orders, pickup-labeled fulfillment groups are handled after rebalance via
     * tryMergePickupFulfillmentOrdersIntoShipping when Shopify allows merging FOs.
     * Otherwise, labels follow each location's delivery profile in Shopify admin.
     */
    async function rebalanceVariantFulfillment(orderId, variantId, targetLocal, targetImported) {
      const maxIterations = 25;
      for (let iter = 0; iter < maxIterations; iter++) {
        const fos = await getFulfillmentOrders(orderId);
        let curLocal = 0;
        let curImported = 0;
        const atLocal = [];
        const atImported = [];

        for (const fo of fos) {
          const loc = String(fo.assigned_location_id);
          for (const foLine of fo.line_items || []) {
            if (String(foLine.variant_id) !== String(variantId)) continue;
            const q = Number(
              foLine.fulfillable_quantity ?? foLine.quantity ?? 0
            );
            if (q <= 0) continue;
            if (loc === String(LOCAL)) {
              curLocal += q;
              atLocal.push({ fo, foLine, q });
            } else if (loc === String(IMPORTED)) {
              curImported += q;
              atImported.push({ fo, foLine, q });
            }
          }
        }

        if (curLocal === targetLocal && curImported === targetImported) {
          console.log(
            `✅ Fulfillment balanced for variant ${variantId}: Local ${curLocal}, Imported ${curImported}`
          );
          return;
        }

        const needToLocal = targetLocal - curLocal;
        if (needToLocal > 0) {
          for (const { fo, foLine, q } of atImported) {
            const moveQty = Math.min(needToLocal, q);
            if (moveQty <= 0) continue;
            console.log(
              `🚚 Move ${moveQty} of variant ${variantId} IMPORTED → LOCAL (need ${needToLocal})`
            );
            await moveFulfillmentLineItem(fo.id, foLine.id, moveQty, LOCAL);
            break;
          }
          continue;
        }

        const needToImported = targetImported - curImported;
        if (needToImported > 0) {
          for (const { fo, foLine, q } of atLocal) {
            const moveQty = Math.min(needToImported, q);
            if (moveQty <= 0) continue;
            console.log(
              `🚚 Move ${moveQty} of variant ${variantId} LOCAL → IMPORTED (need ${needToImported})`
            );
            await moveFulfillmentLineItem(fo.id, foLine.id, moveQty, IMPORTED);
            break;
          }
          continue;
        }

        console.log(
          `⚠️ Cannot fully rebalance variant ${variantId}: targets Local ${targetLocal} Imported ${targetImported}, current Local ${curLocal} Imported ${curImported}`
        );
        return;
      }
    }

    // ================================================================
    // 🔥 HANDLER: orders/create
    // ================================================================
    async function handleOrderCreate() {
      const orderId = data.id;
      const routingMode = getEffectiveRoutingMode(data);
      const splitPlan = {
        __meta: {
          inventoryAdjusted: false,
          routingMode:
            routingMode === "local_first"
              ? "local_first_fulfillment"
              : "imported_first_fulfillment",
        },
      };
      const fulfillmentOrders = await getFulfillmentOrdersWithRetry(orderId);
      const assignedByVariant = buildAssignedQtyByVariant(fulfillmentOrders);

      for (const item of data.line_items || []) {
        console.log("🍷 Product:", item.title);
        console.log("Qty:", item.quantity);

        if (!item.variant_id) {
          console.log("⏭ Skip: no variant_id");
          continue;
        }

        if (!item.variant_inventory_management) {
          console.log("⏭ Skip: no inventory tracking");
          continue;
        }

        const inventoryItemId = await getInventoryItemId(item.variant_id);
        if (!inventoryItemId) {
          console.log("⏭ Skip: inventory_item_id not found");
          continue;
        }

        const orderedQty = item.quantity;
        const localQ = await getInventoryQuantities(inventoryItemId, LOCAL);
        const importedQ = await getInventoryQuantities(inventoryItemId, IMPORTED);

        const assigned = assignedByVariant[String(item.variant_id)] || {
          local: 0,
          imported: 0,
        };
        const localAvailable = Number(localQ.available || 0);
        const importedAvailable = Number(importedQ.available || 0);
        const localCapacity = Math.max(0, localAvailable + assigned.local);
        const importedCapacity = Math.max(0, importedAvailable + assigned.imported);

        console.log(
          "Local (on_hand/available/committed):",
          localQ.on_hand,
          localQ.available,
          localQ.committed
        );
        console.log(
          "Imported (on_hand/available/committed):",
          importedQ.on_hand,
          importedQ.available,
          importedQ.committed
        );
        console.log(
          `Current order assigned from FO (local/imported): ${assigned.local}/${assigned.imported}`
        );
        console.log(
          `Reconstructed capacity (local/imported): ${localCapacity}/${importedCapacity}`
        );

        const { localQty, importedQty } =
          routingMode === "local_first"
            ? computeLocalFirstSplit(orderedQty, localCapacity, importedCapacity)
            : computeImportedFirstSplit(orderedQty, localCapacity, importedCapacity);

        if (localQty + importedQty < orderedQty) {
          console.log(
            `⚠️ Not enough stock across locations for "${item.title}": ordered ${orderedQty}, plan local ${localQty} + imported ${importedQty}`
          );
        }

        splitPlan[item.variant_id] = { localQty, importedQty };
      }

      console.log("📊 Split plan:", JSON.stringify(splitPlan));

      if (!fulfillmentOrders.length) {
        console.log("⚠️ No fulfillment orders yet; skip rebalance (retry exhausted).");
        await removeSplitPlanNoteAttribute(orderId);
        return res.status(200).send("OK");
      }

      for (const key of Object.keys(splitPlan)) {
        if (key === "__meta") continue;
        const plan = splitPlan[key];
        const targetLocal = Number(plan.localQty || 0);
        const targetImported = Number(plan.importedQty || 0);
        if (targetLocal === 0 && targetImported === 0) continue;
        await rebalanceVariantFulfillment(orderId, key, targetLocal, targetImported);
      }

      await tryMergePickupFulfillmentOrdersIntoShipping(orderId);

      // Do not persist split plan on note_attributes (Additional details). Strip if present.
      await removeSplitPlanNoteAttribute(orderId);

      return res.status(200).send("OK");
    }

    // ================================================================
    // 🔥 HANDLER: orders/cancelled
    // ================================================================
    async function handleCancellation() {
      const orderId = data.id;
      console.log("❌ Order cancelled:", orderId);

      // ✅ PRIMARY: Use stored split plan
      const splitPlan = getSplitPlanFromNoteAttributes(data.note_attributes);

      if (splitPlan && Object.keys(splitPlan).length > 0) {
        const wasInventoryAdjusted =
          splitPlan?.__meta?.inventoryAdjusted === undefined
            ? true
            : Boolean(splitPlan.__meta.inventoryAdjusted);

        if (!wasInventoryAdjusted) {
          console.log("⏭ Skipping manual restock (inventory was not manually adjusted on create).");
          return res.status(200).send("OK");
        }

        console.log("📦 Using stored split plan for restock");

        for (const item of data.line_items || []) {

          const variantId = item.variant_id;
          const plan = splitPlan[variantId];

          if (!variantId || !plan) continue;

          const inventoryItemId = await getInventoryItemId(variantId);
          if (!inventoryItemId) continue;

          const localQty = Number(plan.localQty || 0);
          const importedQty = Number(plan.importedQty || 0);

          if (localQty > 0) {
            console.log(`♻️ Restocking ${localQty} of "${item.title}" to Local Stock`);
            await adjustInventory(inventoryItemId, LOCAL, +localQty);
            console.log(`✅ Restocked ${localQty} to Local Stock`);
          }

          if (importedQty > 0) {
            console.log(`♻️ Restocking ${importedQty} of "${item.title}" to Imported Stock`);
            await adjustInventory(inventoryItemId, IMPORTED, +importedQty);
            console.log(`✅ Restocked ${importedQty} to Imported Stock`);
          }
        }

        return res.status(200).send("OK");
      }

      // ⚠️ FALLBACK: Use refund_line_items if no split plan saved
      console.log("⚠️ No split plan found. Using refund_line_items fallback.");

      const refunds = data.refunds || [];

      for (const refund of refunds) {
        for (const refundItem of refund.refund_line_items || []) {

          if (refundItem.restock_type !== "cancel") continue;

          const locationId = refundItem.location_id;
          const qty = refundItem.quantity;
          const variantId = refundItem.line_item?.variant_id;
          const productName = refundItem.line_item?.name;

          if (!variantId) continue;

          const inventoryItemId = await getInventoryItemId(variantId);
          if (!inventoryItemId) continue;

          const locationName = String(locationId) === String(LOCAL)
            ? "Local Stock"
            : "Imported Stock";

          console.log(`♻️ Restocking ${qty} of "${productName}" to ${locationName}`);
          await adjustInventory(inventoryItemId, locationId, +qty);
          console.log(`✅ Restocked ${qty} to ${locationName}`);
        }
      }

      return res.status(200).send("OK");
    }

    // ================================================================
    // 🔀 ROUTE by topic
    // ================================================================
    if (topic === "orders/create") {
  return await handleOrderCreate();
}

if (topic === "orders/cancelled") {
  return await handleCancellation();
}

console.log("⏭ Ignored webhook topic:", topic);
return res.status(200).send("Ignored");

  } catch (error) {
    console.error("❌ ERROR:", error);
    return res.status(200).send("Error handled");
  }
}
