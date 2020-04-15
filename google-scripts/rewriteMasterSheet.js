function rewriteMasterSheet() {
  // (1) Open the Master Sheet and clear its contents.
  var masterSheet = SpreadsheetApp.openById(MASTER_RESPONSES_SHEET);
  clearMasterSheet(masterSheet);
  
  // (2) Open member id mappings sheet and convert to an object
  var memberSheets = SpreadsheetApp.openById(MEMBER_MAPPINGS_SHEET);
  var memberSheetsData = memberSheets.getDataRange().getValues();
  var memberSheetsData = sheetToObjs(getMemberMappingsSheet());
  
  for (var idx in memberSheetsData) {
    var memberSheetId = memberSheetsData[idx]["Spreadsheet ID"]
    appendMemberRows(memberSheetId, masterSheet);
  }

}

function clearMasterSheet(masterSheet) {
  /** 
  This function deletes all rows in the masterSheet
  with the exception of the header row.
  */
  var rowPosition = 2;
  var howMany = masterSheet.getLastRow() - 1;
  
  masterSheet.deleteRows(rowPosition, howMany);
}

function appendMemberRows(memberSheetId, masterSheet) {
  /**
  This function retrieves data from a specified memberSheet and
  appends the data to the masterSheet.
  */
  var memberSheet = SpreadsheetApp.openById(memberSheetId);
  var data = memberSheet.getDataRange().getValues();
    
  for (var i = 1; i < data.length; i++) {
    masterSheet.appendRow(data[i])
  }
}
