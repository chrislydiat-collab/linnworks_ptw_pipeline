function main_fullPurchaseOrders_toSheet() {
  const allData = getLinnworksPurchaseOrdersDataFull(); // fetch ALL POs
  if (allData.length === 0) {
    Logger.log('No Linnworks purchase order data to write.');
    return;
  }
  Logger.log(`Flattened rows ready for sheet: ${allData.length}`);
  writeDataToSheet(allData, "FullPurchaseOrders");
}

function writeDataToSheet(data, sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);

  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  } else {
    sheet.clear(); // clear old data
  }

  const columns = Object.keys(data[0]);
  const values = [columns];

  data.forEach(row => {
    const rowData = columns.map(col => row[col]);
    values.push(rowData);
  });

  sheet.getRange(1, 1, values.length, columns.length).setValues(values);
  Logger.log(`Written ${data.length} rows to sheet "${sheetName}".`);
}

function getLinnworksAccessToken() {
  const url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication";
  const payload = {
    ApplicationId: "ApplicationId",
    ApplicationSecret: "ApplicationSecret",
    Token: "Token"
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const data = JSON.parse(response.getContentText());
  const token = data.Token || data.AccessToken;
  if (!token) {
    Logger.log("Access token not found: " + response.getContentText());
    return null;
  }
  return token;
}

function getLinnworksPurchaseOrdersData() {
  const accessToken = getLinnworksAccessToken();
  if (!accessToken) return [];

  const payload = {
    searchParameters: {
      DateFrom: '2020-01-01T00:00:00',
      DateTo: new Date().toISOString()
    },
    entriesPerPage: 100,
    pageNumber: 1
  };

  const headers = {
    'Authorization': accessToken,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  };

  const options = {
    method: 'POST',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  let allData = [];
  let currentPage = 1;
  let totalPages = 1;

  do {
    payload.pageNumber = currentPage;
    options.payload = JSON.stringify(payload);

    const response = UrlFetchApp.fetch('https://eu-ext.linnworks.net/api/PurchaseOrder/Search_PurchaseOrders2', options);
    const json = JSON.parse(response.getContentText());

    if (json.Result && json.Result.length > 0) {
      allData = allData.concat(json.Result);
    }

    totalPages = json.TotalPages || 1;
    currentPage++;
  } while (currentPage <= totalPages);

  return allData;
}

function getLinnworksPurchaseOrderDetails(pkPurchaseID, token) {
  const url = "https://eu-ext.linnworks.net/api/PurchaseOrder/Get_PurchaseOrder";
  const options = {
    method: 'POST',
    headers: {
      'Authorization': token,
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify({ pkPurchaseId: pkPurchaseID }),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  if (response.getResponseCode() !== 200) {
    Logger.log("Error fetching PO details for " + pkPurchaseID + ": " + response.getContentText());
    return null;
  }
  return JSON.parse(response.getContentText());
}

function getLinnworksPurchaseOrdersDataFull() {
  const accessToken = getLinnworksAccessToken();
  if (!accessToken) return [];

  const allOrdersSummary = getLinnworksPurchaseOrdersData();
  if (allOrdersSummary.length === 0) return [];

  const allRows = [];
  allOrdersSummary.forEach(order => {
    Logger.log("Fetching details for pkPurchaseID: " + order.pkPurchaseID);
    const poDetails = getLinnworksPurchaseOrderDetails(order.pkPurchaseID, accessToken);
    if (poDetails) {
      const flattened = flattenPurchaseOrder(poDetails);
      allRows.push(...flattened);
    }
  });

  return allRows;
}

function flattenPurchaseOrder(po) {
  const header = po.PurchaseOrderHeader || {};
  const items = po.PurchaseOrderItem || [];
  const delivered = po.DeliveredRecords || [];

  const rows = [];

  items.forEach(item => {
    const matchedDeliveries = delivered.filter(dr => dr.fkPurchaseItemId === item.pkPurchaseItemId);
    if (matchedDeliveries.length > 0) {
      matchedDeliveries.forEach(dr => {
        rows.push(buildRow(item, header, dr));
      });
    } else {
      rows.push(buildRow(item, header, {}));
    }
  });

  return rows;
}

function buildRow(item, header, delivery) {
  return {
    "PurchaseOrderItem.pkPurchaseItemId": item.pkPurchaseItemId,
    "PurchaseOrderItem.fkStockItemId": item.fkStockItemId,
    "PurchaseOrderItem.StockItemIntId": item.StockItemIntId,
    "PurchaseOrderItem.Quantity": item.Quantity,
    "PurchaseOrderItem.Cost": item.Cost,
    "PurchaseOrderItem.Delivered": item.Delivered,
    "PurchaseOrderItem.TaxRate": item.TaxRate,
    "PurchaseOrderItem.Tax": item.Tax,
    "PurchaseOrderItem.PackQuantity": item.PackQuantity,
    "PurchaseOrderItem.PackSize": item.PackSize,
    "PurchaseOrderItem.SKU": item.SKU,
    "PurchaseOrderItem.ItemTitle": item.ItemTitle,
    "PurchaseOrderItem.InventoryTrackingType": item.InventoryTrackingType,
    "PurchaseOrderItem.IsDeleted": item.IsDeleted,
    "PurchaseOrderItem.SortOrder": item.SortOrder,
    "PurchaseOrderItem.DimHeight": item.DimHeight,
    "PurchaseOrderItem.DimWidth": item.DimWidth,
    "PurchaseOrderItem.BarcodeNumber": item.BarcodeNumber,
    "PurchaseOrderItem.DimDepth": item.DimDepth,
    "PurchaseOrderItem.BoundToOpenOrdersItems": item.BoundToOpenOrdersItems,
    "PurchaseOrderItem.QuantityBoundToOpenOrdersItems": item.QuantityBoundToOpenOrdersItems,
    "PurchaseOrderItem.SupplierCode": item.SupplierCode,
    "PurchaseOrderItem.SupplierBarcode": item.SupplierBarcode,
    "PurchaseOrderItem.SkuGroupIds": JSON.stringify(item.SkuGroupIds || []),
    "PurchaseOrderHeader.pkPurchaseID": header.pkPurchaseID,
    "PurchaseOrderHeader.ExternalInvoiceNumber": header.ExternalInvoiceNumber,
    "PurchaseOrderHeader.Status": header.Status,
    "PurchaseOrderHeader.DateOfPurchase": header.DateOfPurchase,
    "PurchaseOrderHeader.DateOfDelivery": header.DateOfDelivery,
    "PurchaseOrderHeader.TotalCost": header.TotalCost,
    "DeliveredRecords.pkDeliveryRecordId": delivery.pkDeliveryRecordId || null,
    "DeliveredRecords.fkPurchaseItemId": delivery.fkPurchaseItemId || null,
    "DeliveredRecords.fkStockLocationId": delivery.fkStockLocationId || null,
    "DeliveredRecords.UnitCost": delivery.UnitCost || null,
    "DeliveredRecords.DeliveredQuantity": delivery.DeliveredQuantity || null,
    "DeliveredRecords.CreatedDateTime": delivery.CreatedDateTime || null,
    "DeliveredRecords.fkBatchInventoryId": delivery.fkBatchInventoryId || null,
    "DeliveredRecords.ModifiedDateTime": delivery.ModifiedDateTime || null
  };
}
