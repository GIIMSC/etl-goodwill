/**
This file contains utility functions used in both Code.gs and rewriteMasterSheet.gs.
*/

function getMemberMappingsSheet() {
  return SpreadsheetApp.openById(MEMBER_MAPPINGS_SHEET);
}


function sheetToObjs(sheet) {
  var values = sheet.getDataRange().getValues();

  var data = [];

  // Start at row 1 to avoid headers
  for (var i = 1; i < values.length; i++) {
    var rowData = {};
    for (var j = 0; j < values[i].length; j++) {
      if (values[i][j]) {
        rowData[values[0][j]] = values[i][j];
      }
    }
    if (Object.keys(rowData).length > 0) {
      data.push(rowData);
    }
  }

  return data;
}
