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

# internal function to parse int and float in json response from big query
#
# returns a dataframe
function _basic_type_converter(df)
  float_col = []
  int_col = []
  for i in names(df)
      try
          if eltype(tryparse.(Int, collect(skipmissing(df[!, i])))) == Int
              int_col = vcat(int_col, i)
          elseif eltype(tryparse.(Float64, collect(skipmissing(df[!, i])))) == Float64
              float_col = vcat(float_col, i)
          end
      catch
      end
  end
  try
      if length(int_col) > 0
          transform!(df, int_col .=> ByRow(x -> passmissing(parse)(Int, x)) .=> int_col)
      end
      if length(float_col) > 0
          transform!(df, float_col .=> ByRow(x -> passmissing(parse)(Float64, x)) .=> float_col)
      end
  catch e
      println(e)
  end
  return df
end

# Execute a query
#
# Returns a dataframe
function gbq_query(query; convert_numeric=false, use_legacy_sql=false, quiet=true, max_rows=100000000)
  response = JSON.parse(read(`bq --format=json  --quiet="$quiet" query --use_legacy_sql="$use_legacy_sql" --max_rows="$max_rows" "$query"`, String))
  if convert_numeric == true
    return _basic_type_converter(_gbq_parse(response))  
  else
    return _gbq_parse(response)
  end
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
function gbq_list_tables(dataset::AbstractString)
  response = JSON.parse(read(`bq ls --format=json "$dataset"`, String))
  tables = DataFrame(table_id=[table["tableReference"]["tableId"] for table in response],
                     creation_time=[table["creationTime"] for table in response],
                     id=[table["id"] for table in response])
  return tables
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

# upload a table
#
# makes a temporary JSON file from a dataframe and sends it to BigQuery
function gbq_upload(df::DataFrame, project::AbstractString, dataset::AbstractString, table::AbstractString, overwrite::Bool=false, append::Bool=false)
  # write to temporary JSON file
  temp_file = joinpath(tempdir(), "temp.json")
  open(temp_file,"w") do f
      JSON.print(f, df)
  end
  
  # build command to upload JSON to BigQuery
  if overwrite
      cmd = `bq load --autodetect --quiet=true --project_id=$project --source_format=NEWLINE_DELIMITED_JSON --replace=true $dataset.$table $temp_file`
  else append
      cmd = `bq load --autodetect --quiet=true --project_id=$project --source_format=NEWLINE_DELIMITED_JSON --replace=false $dataset.$table $temp_file`
  end
  
  # run command and capture output
  output = read(cmd, String)
  
  # delete temporary file
  rm(temp_file)
  
  return output
end


end
