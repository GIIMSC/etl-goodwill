# etl-goodwill
This repo contains the necessary elements to collect, manage, and store Goodwill programs data. 

## Collecting the Programs Data 
*This solution, devised by the Google Fellows (2018), originated in the [GoodwillDataInitiative](https://github.com/GIIMSC/GoodwillDataInitiative).*

Local Goodwills collect programs data using [a Google form](https://docs.google.com/forms/d/12oKuKov-yvhhIMxgYhn3HjXUj1TM_dbp9a8tSuOVf7k/edit). The form connects to a Google sheet. This sheet (which stores all incoming form data) runs a custom script every time someone submits the form. The script writes the form data to local-Goodwill-specific Google sheets. The ETL process pulls data from these Goodwill-specific sheets.

### Relevant files
The relevant forms, sheets, and scripts mostly live in the IDC Documentation Google Drive managed by `idc_pipeline@goodwill.org`.

* [Form: Google Programs Data](https://docs.google.com/forms/d/12oKuKov-yvhhIMxgYhn3HjXUj1TM_dbp9a8tSuOVf7k/edit) – completed by local Goodwills (once per program entry).
* [Master Google sheet: All form responses](https://docs.google.com/spreadsheets/d/1eWyIIFjZs6A4-w0JsdVGU_bwtWPPsL94ZbdYJ6RvoeI/edit#gid=1219291451) – stores all form responses, and provides the current, accurate headers for all sheets. Ultimately, it is just an intermediary point between the form and the local sheets.
* [Local Goodwill sheets](https://drive.google.com/drive/u/3/folders/1rYdxJb_ICOAwkgy6IVAK5LNEi3E7Pv4T) – stand as the "source of truth" for programs data. Local Goodwills edit program data on these local sheets, i.e., NOT on the master sheet.
* [Mappings: Member name --> Sheet ID](https://docs.google.com/spreadsheets/d/1WDyh5jwRUWEa2WQy-np7Bd3oaiNPU38_uJRtpxzJrp4/edit#gid=0) – referenced in the Google script, so that the script knows where to write form data
* [Documentation for onboarding a local Goodwill](https://docs.google.com/document/d/1ZFwhBb0d_0BDrmW2zD3KBkjx709bMpm3-Bf_3eNGix4/edit#heading=h.sfq8kx6h25fc)
* [Google Fellows Presentation](https://docs.google.com/presentation/d/1-Q6vhtMBa8MwqNOoyVpGWntJLQFRQnzt5sgh3T4HLKY/edit#slide=id.g562a49c13a_0_96)
* [Project scripts](https://script.google.com/u/3/home/all) - stores all Goodwill project scripts. Users must sign in as `idc_pipeline@goodwill.org`.

Development versions of the form, sheets, and scripts live in [a designated Google folder](https://drive.google.com/drive/u/3/folders/1i6gjLx8dsjkzjpq18lKw8fsT9-zDIDLx). Use this for TESTING changes in the form or script before pushing to production (see below).

### Managing programs data

What if a local Goodwill needs to update a program? or makes a mistake filling out the form? BrightHive developers and local Goodwills should abide by the following:

- the Google form does not enable "Edit after submit": it should stay as such;
- do not edit the master google sheet
- do not change or delete the `Row Identifier`
- make any and all edits in the local Google sheets

### Changing the Google scripts

1. Adjust the **development** script by visiting [the project dashboard](https://script.google.com/u/3/home/projects/1qIEL-AYTGqrcPCpsbByS9DNffBqPsjuhepvXDoP9jzsvtAu2KEGigyRb) and [the script itself](https://script.google.com/a/goodwill.org/d/1qIEL-AYTGqrcPCpsbByS9DNffBqPsjuhepvXDoP9jzsvtAu2KEGigyRb/edit).
2. Test changes by filling out [the **development** form](https://docs.google.com/forms/u/3/d/1evR-Ryqc7i5G7y-096LMCZAxNlJDkh7GgaXHbKsX-UA/edit?usp=drive_web) and checking [the script logs for completion](https://script.google.com/u/3/home/projects/1qIEL-AYTGqrcPCpsbByS9DNffBqPsjuhepvXDoP9jzsvtAu2KEGigyRb/executions?run_as=1). Also, inspect the [**development** master sheet](https://docs.google.com/spreadsheets/d/1AydXkq6Y-LtuQJO0jz4KXgUDOiaWnbRkaSEFIP5tmb0/edit#gid=764851746) and corresponding local sheet.
3. Happy with the changes? Then, pull them from Google scripts with [`clasp`](https://developers.google.com/apps-script/guides/clasp).

```
# install clasp 
npm install @google/clasp -g

# Move into the `google-scripts` directory
cd google-scripts

# login to to clasp as `idc_pipeline@goodwill.org`
clasp login

# clone programs scripts to this repo
clasp pull
```

4. Commit your changes (on a branch), and push to Github for code review.
5. Merge to master. CircleCI will push the changes to the production version of the Google scripts.

## Extracting, Transforming, and Loading Programs Data

The scripts in this repo port data to the [Data Resource API](https://github.com/brighthive/data-resource-api) that hosts the schema for [Goodwill programs data](https://github.com/brighthive/goodwill-data-resource-schema). 

### 1. Stand up a Data Resource API
Follow the instructions in the [Data Resource API README](https://github.com/brighthive/data-resource-api). Use the schema described in [goodwill-data-resource-schema](https://github.com/brighthive/goodwill-data-resource-schema)!

### 2. Add a config.py

`etl-goodwill` has a light-weight config file, which tells the script how to connect to Google sheets and the `psql` database.

```
touch config/config.py
```

Then, ask a friendly BrightHive developer for config specifics.

### 3. Launch a virtualenv

BrightHive recommends managing virtual environments with `pipenv`.

```
# tell pipenv to create/update a virtual env
pipenv install

# instantiate a shell within the virtualenv
pipenv shell
```

### 4. Run the script

You're ready!

```
python runner.py
```

Then, head over to the Data Resource API (e.g., `http://localhost:8000/programs` or `http://goodwill-programs-testing.brighthive.net/programs`) and view the newly imported programs.

## 