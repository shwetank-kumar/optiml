# optiml

Repo: https://github.com/shwetank-kumar/optiml.git

Dev rules for queries:
1. If its a timeseries append _ts at the end of the filename
2. ts should have data and time stamp
3. Make the API of each of the timeseries in cost consistent since they will be called by similar python functions down the road


## ToDos
1. "Manual record creation history"
2. "pip install "snowflake-connector-python[pandas]""
### Dev Plans ###
1. Replan if anything on Manas
2. 
### Customer Conv ###

### Dashboard Feedbacks 
1. Rename dashboard to --- Resource Usage
2. Empty page - Query Analysis, WH Profiling, Storage Profiling, User Analysis 
3. Input for Data based on time window.
4. Top Metric must be there.

### Usage

Create your conda env locally:
`conda env create -f env/env.yml`

Activate the environment `conda activate [name]`

Run jupyter lab from cmd line: `jupyter-lab`

Create an `env/.env` file with creds for notebooks to load:
e.g.
```
export SNOWFLAKE_ACCOUNT='OMWYKHW-ENTERPRISE'
export SNOWFLAKE_USER='saf'
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_ROLE='DEV'
export SNOWFLAKE_DATABASE='SANDBOX'
export SNOWFLAKE_SCHEMA='PUBLIC'
export SNOWFLAKE_WAREHOUSE='DEMO'
```
