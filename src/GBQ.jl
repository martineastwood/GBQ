module GBQ

# external library dependancies
using JSON
using DataFrames


# functions to export
export gbq_large_results, gbq_show, gbq_head, gbq_list_tables,
gbq_list_datasets, gbq_list_projects, gbq_query, gbq_create_dataset,
gbq_copy_table, gbq_table_exists


# custom exception for GBQ package
struct GBQException <: Exception
    var::String
end

Base.showerror(io::IO, e::GBQException) = print(io, e.var);


# runs a query and saves results directly into a table
# query = the query to run
# destination = destination table (must not already exist)
# table = dataset to store table in
#
# returns DataFrame conatining a preview of the new table
function gbq_large_results(query, project, dataset, table)
  if gbq_table_exists(dataset, table)
    throw(GBQException("Destination Table Already Exists"))
  end
  response = JSON.parse(read(`bq --format=json --quiet=True query --destination_table=$project:$dataset.$table "$query"`, String))
  return _gbq_parse(response)
end


# check if table exists in dataset
#
# returns boolean
function gbq_table_exists(dataset, table)
    tables = gbq_list_tables(dataset)
    return table in tables[:tableId]
end


# internal function to parse json response returned from big query
#
# returns a dataframe
function _gbq_parse(response)
    cols = collect(keys(response[1]))
    values = Dict()
    for key in cols
        values[key] = []
    end
    for dict in response
        for key in cols
            push!(values[key], dict[key])
        end
    end
    return DataFrame(values)
end


# Execute a query
#
# Returns a dataframe
function gbq_query(query; use_legacy_sql=false, quiet=true, max_rows=100000000)
  response = JSON.parse(read(`bq --format=json  --quiet="$quiet" query --use_legacy_sql="$use_legacy_sql" --max_rows="$max_rows" "$query"`, String))
  return _gbq_parse(response)
end


# list all projects associated with default billing account
#
# Returns a dataframe
function gbq_list_projects()
  response = JSON.parse(read(`bq ls --format=json -p`, String))
  return _gbq_parse(response)
end


# lists datasets in default project
#
# Returns a dataframe
function gbq_list_datasets()
  response = JSON.parse(read(`bq ls --format=json`, String))
  return _gbq_parse(response)
end


# lists all tables in a specified dataset
#
# Returns a dataframe
function gbq_list_tables(dataset)
  response = JSON.parse(read(`bq ls --format=json "$dataset"`, String))
  return _gbq_parse(response)
end


# preview the head of the table
#
# Returns a dataframe
function gbq_head(dataset, table, num_rows=10)
  response = JSON.parse(read(`bq --format=json head -n $num_rows $dataset.$table`, String))
  return _gbq_parse(response)
end


# examine the schema for a table
#
# Returns dict containing schema
function gbq_show(dataset, table)
  return JSON.parse(read(`bq --format=json show $dataset.$table`, String))
end


# create a new dataset in the default project
#
# returns string containing response from Google BigQuery
function gbq_create_dataset(dataset)
  return read(`bq mk $dataset`, String)
end


# copy a table
#
# returns string containing response from Google BigQuery
function gbq_copy_table(dataset1, table1, dataset2, table2)
  return read(`bq cp --quiet=True $dataset1.$table1 $dataset2.$table2`, String)
end

end
