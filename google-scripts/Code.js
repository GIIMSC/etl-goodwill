function onSubmit(e) {
  Logger.log("%s", JSON.stringify(e));
  var memberId = String(e.namedValues['Goodwill Member Name']).trim();

  if (!memberId) {
    return;
  }
  var masterResponsesSheet = SpreadsheetApp.openById(MASTER_RESPONSES_SHEET).getActiveSheet();
  var masterHeaders = masterResponsesSheet.getRange(1, 1, 1, masterResponsesSheet.getMaxColumns());
  var members = sheetToObjs(getMemberMappingsSheet());
  const [memberSheetId, shortName] = getMemberSheet(members, memberId);

  var email;
  var subject;
  if (memberSheetId) {
    var uniqueRowIdentifier = create_UUID()
    var newRowValues = e.values
    newRowValues.push(uniqueRowIdentifier);

    if (e.namedValues["Program ID"][0] === "") {
      // This code updates `newRowValues` with a "Program ID" (i.e., index 7 of the array).
      //
      // Note! We know the "Program ID" occurs in the 7th position, because Column F
      // in the spreadsheet stores the "Program ID."
      // This could be determined dynamically, but since the sheet column order should remain
      // largely static, it is not worth the effort to do so.
      var randomNum = Math.floor(Math.random()*9000) + 1000;
      var generatedProgramID = shortName + "-" + randomNum;
      newRowValues[7] = generatedProgramID;
    }

    var memberSheet = copyToMemberSheet(memberSheetId, masterHeaders, newRowValues);

    subject = "Thank you for submitting the Goodwill Programs Data form!";
    email = "Thank you for submitting program data. You can edit your responses by visiting your local programs sheet: " + memberSheet.getUrl() + ". Your responses are shown below.";
  } else {
    subject = "There has been a problem with your form submission";
    email = "Goodwill organization '" + memberId + "' has not been set up to enable programs data submission. " +
      "Your responses are shown below.";
  }
  email += "<br><br>" + constructEmailBody(FormApp.openByUrl(masterResponsesSheet.getFormUrl()), e.namedValues);

  MailApp.sendEmail({
    to: String(e.namedValues['Your email address']).trim(),
    subject: subject,
    htmlBody: email,
    noReply: true
  });
}

function getMemberSheet(members, memberId) {
  for (var i = 0; i < members.length; i++) {
    if (members[i]["Location"] == memberId) {
      return [members[i]["Spreadsheet ID"], members[i]["Short Name"]];
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
      // Questions not in the form response are section headers.
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
