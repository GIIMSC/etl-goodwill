# etl-goodwill
Scripts for the Goodwill programs data ETL process

## Get started

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
