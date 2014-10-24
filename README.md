# GBQ

The `GBQ` package provides a wrapper around Google's BigQuery command line tool allowing it to be used from within Julia


## Prerequisites

You must have a working installation of the Google SDK and the Google BigQuery command line tool. Information about how to install them and set up authorisation, default projects etc can be found at:

* https://cloud.google.com/bigquery/bq-command-line-tool
* https://developers.google.com/api-client-library/python/start/installation



## Dependancies

`GBQ` uses the Julia [DataFrames](https://github.com/JuliaStats/DataFrames.jl) and [JSON](https://github.com/JuliaLang/JSON.jl) packages


## Installation

`GBQ` can be installed through Julia using Pkg.clone

```
Pkg.clone("https://github.com/martineastwood/GBQ.git")
```

## Examples
```
using GBQ

### query an existing data table
gbq_query("select * from my_dataset.my_table where foo = 'bar'") 

### store results for a large query directly into a new table in a specified dataset and project
gbq_large_results("select * from my_dataset.my_table where foo = 'bar'", my_project, my_dataset, my_table) 

### create a new dataset in default project
gbq_create_dataset("my_new_dataset") 

### list all the tables in a specified dataset
gbq_list_tables("my_dataset") 

### list all datasets in the default project
gbq_list_datasets("my_dataset") 

### create a new dataset in default project
gbq_create_dataset("my_new_dataset") 

### get schema for table in a specified dataset
gbq_show(my_dataset, my_table) 

### preview head of table
gbq_head(my_dataset, my_table) 

### list all projects associated to billing account
gbq_list_projects() 

### copy table from one dataset to another
gbq_copy_table(my_old_dataset, my_old_table, my_new_dataset, my_new_table)

### check if table exists
gbq_table_exists("my_table")

```
