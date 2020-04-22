function gastTestRunner() {
  if ((typeof GasTap)==='undefined') { // GasT Initialization. (only if not initialized yet.)
    eval(UrlFetchApp.fetch('https://raw.githubusercontent.com/zixia/gast/master/src/gas-tap-lib.js').getContentText())
  } // Class GasTap is ready for use now!

  var log = "";
  var loggerFunc = function (msg) { log += msg + "\n" }

  var test = new GasTap({
    logger: loggerFunc
  })

  testMemberMappingsSheetExists(test);
  testSheetToObjs(test);
  testConstructEmailBody(test);

  test.finish();
  Logger.log(log);
  return { failures: test.totalFailed(), log: log }
}

function testMemberMappingsSheetExists(test) {
  test('testMemberMappingsSheetExists', function(t) {
    var sheet = getMemberMappingsSheet();
    t.ok(sheet, "mappings sheet exists");
  });
}

function testSheetToObjs(test) {
  test('testSheetToObjs', function(t) {
    var sheet = SpreadsheetApp.openById("19J5yaeojSqrcVcs0yUbj7qZjBsDWcVuX03d8om_yp9I");
    var objs = sheetToObjs(sheet);

    t.equal(objs.length, 2, "2 objs created from sheet");
    t.equal(Object.keys(objs[0]).length, 3, "3 items in row 1")
    t.equal(objs[0]["Spreadsheet ID"], "139uaTMu3TlfqZLzQB8fI7_5jRkHrEjqLDJ5QAgGp330", "member one ss");
    t.equal(objs[0]["Location"], "Somewhere, CA", "member one loc");
  });
}

function testConstructEmailBody(test) {
  test('testConstructEmailBody', function(t) {
    var testForm = FormApp.openById("1rhs-y4VsRuL9q74-ErLlPOxu_msgVqp6-rv97bcvWSA");
    var testResponses = {"Goodwill Member Name": "Somewhere, CA", "Format": "In person, Online", "Should this program be available in Google Pathways?": "Yes"};

    var expectedEmailBody = "<b>Goodwill Info</b><br>Goodwill Member Name: Somewhere, CA<br>Should this program be available in Google Pathways?: Yes<br>Format: In person, Online<br>";

    t.equal(constructEmailBody(testForm, testResponses), expectedEmailBody, "email matches expected");
  });
}
