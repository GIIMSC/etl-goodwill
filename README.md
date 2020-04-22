# etl-goodwill
This repo contains the necessary elements to collect, manage, and store Goodwill programs data.

## Collecting the Programs Data
*This solution, devised by the Google Fellows (2019), originated in the [GoodwillDataInitiative](https://github.com/GIIMSC/GoodwillDataInitiative).*

Local Goodwills collect programs data using [a Google form](https://docs.google.com/forms/d/12oKuKov-yvhhIMxgYhn3HjXUj1TM_dbp9a8tSuOVf7k/edit). The form connects to a "master" Google sheet, and upon form submission, a custom script populates the master sheet and also writes the form data to a local-Goodwill-specific Google sheet (determined by the form value given in "Goodwill Member ID").

### Relevant files
The relevant forms, sheets, and scripts mostly live in the IDC Documentation Google Drive managed by `idc_pipeline@goodwill.org`.

* [Form: Google Programs Data](https://docs.google.com/forms/d/12oKuKov-yvhhIMxgYhn3HjXUj1TM_dbp9a8tSuOVf7k/edit) – completed by local Goodwills (once per program entry).
* [Master Google sheet: All form responses](https://docs.google.com/spreadsheets/d/1eWyIIFjZs6A4-w0JsdVGU_bwtWPPsL94ZbdYJ6RvoeI/edit#gid=1219291451) – stores all form responses, and provides the current, accurate headers for all sheets. The ETL scripts pull from this sheet.
* [Local Goodwill sheets](https://drive.google.com/drive/u/3/folders/1rYdxJb_ICOAwkgy6IVAK5LNEi3E7Pv4T) – local Goodwills edit program data on these local sheets, i.e., NOT on the master sheet.
* [Mappings (Member name --> Sheet ID)](https://docs.google.com/spreadsheets/d/1WDyh5jwRUWEa2WQy-np7Bd3oaiNPU38_uJRtpxzJrp4/edit#gid=0) – referenced in `google-scripts/Code.js`: it communicates where `onSubmit` should write form data
* [Documentation for onboarding a local Goodwill](https://docs.google.com/document/d/1ZFwhBb0d_0BDrmW2zD3KBkjx709bMpm3-Bf_3eNGix4/edit#heading=h.sfq8kx6h25fc)
* [Google Fellows Presentation](https://docs.google.com/presentation/d/1-Q6vhtMBa8MwqNOoyVpGWntJLQFRQnzt5sgh3T4HLKY/edit#slide=id.g562a49c13a_0_96)
* [Production version of Google scripts](https://script.google.com/a/goodwill.org/d/1KRHoj07y0IR_brLgAQvUNx73iILdhDUUC9UK04qVqz18OhooOlOY8Vgq/edit) and [other project scripts](https://script.google.com/u/3/home/all) - stores all Goodwill project scripts. Users must sign in as `idc_pipeline@goodwill.org`.

Development versions of the form, sheets, and scripts live in [a designated Google folder](https://drive.google.com/drive/u/3/folders/1i6gjLx8dsjkzjpq18lKw8fsT9-zDIDLx). Use these for TESTING changes in the form or script before pushing to production (see below).

### Managing programs data

What if a local Goodwill needs to update a program? or makes a mistake filling out the form? BrightHive developers and local Goodwills should abide by the following:

- make any and all edits in the local Google sheets
- the Google form does not (and should not) enable "Edit after submit"
- do not edit the master google sheet
- do not change or delete the `Row Identifier`

**[A time-driven trigger](https://developers.google.com/apps-script/guides/triggers/installable)** (similar to a cron job) executes the `rewriteMasterSheet` script on a recurring basis. The `rewriteMasterSheet` script clears the master sheet and copies programs data from the local sheets: this process insures that the master sheet reflects updates made at the local level. BrightHive [manually initialized](https://developers.google.com/apps-script/guides/triggers/installable#managing_triggers_manually) the trigger using the [production Google scripts UI](https://script.google.com/a/goodwill.org/d/1KRHoj07y0IR_brLgAQvUNx73iILdhDUUC9UK04qVqz18OhooOlOY8Vgq/edit) (Edit --> Current Project's Triggers).

### Changing the Google scripts

1. Adjust the *development* script by visiting [the project dashboard](https://script.google.com/u/3/home/projects/1qIEL-AYTGqrcPCpsbByS9DNffBqPsjuhepvXDoP9jzsvtAu2KEGigyRb) and [the script itself](https://script.google.com/a/goodwill.org/d/1qIEL-AYTGqrcPCpsbByS9DNffBqPsjuhepvXDoP9jzsvtAu2KEGigyRb/edit). (You can also access the scripts via the "Tools" dropdown in the[*development* master sheet](https://docs.google.com/spreadsheets/d/1AydXkq6Y-LtuQJO0jz4KXgUDOiaWnbRkaSEFIP5tmb0/edit#gid=764851746).)
2. Test changes by filling out [the *development* form](https://docs.google.com/forms/d/1mnjgEXkmd9ENg8u7oeJp1p6av-yZaCBdh4TPEL97ATk/edit) and checking [the script logs for completion](https://script.google.com/u/3/home/projects/1qIEL-AYTGqrcPCpsbByS9DNffBqPsjuhepvXDoP9jzsvtAu2KEGigyRb/executions?run_as=1). You can see all form data in the [*development* master sheet](https://docs.google.com/spreadsheets/d/1AydXkq6Y-LtuQJO0jz4KXgUDOiaWnbRkaSEFIP5tmb0/edit#gid=764851746) and corresponding local sheet.
3. Happy with the changes? Then, pull them from Google scripts with [`clasp`](https://developers.google.com/apps-script/guides/clasp).

```
# install clasp (if you have not already done so)
npm install @google/clasp -g

# Move into the `google-scripts` directory
cd google-scripts

# login to to clasp as `idc_pipeline@goodwill.org`
clasp login

# clone programs scripts to this repo
clasp pull
```

4. Commit your changes (on a branch), and push to Github for code review.
5. Merge to master. CircleCI will push the changes to the production version of the Google scripts. How? The `Push Google Scripts` job in `.circleci/config.yml` CDs into the `google-scripts` repo, renames the script files for production, and uses `clasp` to push. CircleCi has the proper credentials by way of the `APPS_SCRIPT_CLASPRC` env variable ([saved in the CircleCI `etl-goodwill` project](https://circleci.com/docs/2.0/env-vars/#setting-an-environment-variable-in-a-project)). This environment variable stores login tokens generated at the time of [running `clasp login` and stored in `~/.clasprc.json`](https://www.npmjs.com/package/@google/clasp#login).

## Extracting, Transforming, and Loading Programs Data

It looks like you have some programs data! Hurrah. You can now execute the ETL scripts, which port data to an instance of the [BrightHive Google Pathways API](https://github.com/brighthive/google-pathways-api). Follow these steps to get set up.

### 1. Stand up a Google Pathways API
Follow the instructions in the [BrightHive Google Pathways API](https://github.com/brighthive/google-pathways-api).

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

Then, head over to the Google Pathways API (e.g., `http://localhost:8000/programs`) and view the newly imported programs.
