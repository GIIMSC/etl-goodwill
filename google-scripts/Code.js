function onSubmit(e) {
  console.info(e.namedValues);
  Logger.log(e.namedValues);
  var memberId = String(e.namedValues['Goodwill Member ID']).trim();
  
  if (!memberId) {
    return;
  }
  // `getActiveSheet` returns a Sheet object (in this case, the Master sheet).
  // https://developers.google.com/apps-script/reference/spreadsheet/spreadsheet-app#getactivesheet
  // https://developers.google.com/apps-script/reference/spreadsheet/sheet.html
  var masterResponsesSheet = SpreadsheetApp.openById(MASTER_RESPONSES_SHEET).getActiveSheet();
  var masterHeaders = masterResponsesSheet.getRange(1, 1, 1, masterResponsesSheet.getMaxColumns());  
  var members = sheetToObjs(getMemberMappingsSheet());
  var memberSheetId = getMemberSheet(members, memberId);

  var email;
  var subject;
  if (memberSheetId) {
    var uniqueRowIdentifier = create_UUID()
    var newRowValues = e.values
    newRowValues.push(uniqueRowIdentifier);
    var memberSheet = copyToMemberSheet(memberSheetId, masterHeaders, newRowValues);

    subject = "Thank you for submitting the Goodwill Programs Data form";
    email = "Your responses are shown below. To edit them, go to " + memberSheet.getUrl() + ".";
  } else {
    subject = "There has been a problem with your form submission";
    email = "Member id " + memberId + " has not been set up to enable programs data submission. " +
      "Please check the member ID again. Your responses are shown below.";
  }
  email += "<br><br>" + constructEmailBody(FormApp.openByUrl(masterResponsesSheet.getFormUrl()), e.namedValues);
  
  MailApp.sendEmail({
    to: String(e.namedValues['Your email address']).trim(),
    subject: subject,
    htmlBody: email,
    noReply: true
  });
}

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

function getMemberSheet(members, memberId) {
  for (var i = 0; i < members.length; i++) {  
    if (members[i]["Location"] == memberId) {
      return members[i]["Spreadsheet ID"];
    }
  }
  return null;
}

function copyToMemberSheet(memberSheetId, masterHeaders, newRow) {
  var memberSheet = SpreadsheetApp.openById(memberSheetId);
  var copyHeaders = memberSheet.getActiveSheet().getRange(1, 1, 1, masterHeaders.getNumColumns());
  copyHeaders.setRichTextValues(masterHeaders.getRichTextValues());
  
  memberSheet.appendRow(newRow);
  return memberSheet;
}

function constructEmailBody(form, responses) {
  var items = form.getItems();
  var email = "";
  for (var i = 0; i < items.length; i++) {
    var question = items[i].getTitle();
    if (question in responses) {
      email += question + ": " + responses[question] + "<br>";
    } else {
      // If it's not in the form response, then it was a section header
      email += "<b>" + question + "</b><br>";
    }
  }
  return email;
}

function create_UUID(){
  // https://www.w3resource.com/javascript-exercises/javascript-math-exercise-23.php
  var dt = new Date().getTime();
  var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = (dt + Math.random()*16)%16 | 0;
    dt = Math.floor(dt/16);
    return (c=='x' ? r :(r&0x3|0x8)).toString(16);
  });
  return uuid;
}