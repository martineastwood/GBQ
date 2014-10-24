module GBQ

<<<<<<< HEAD

=======
>>>>>>> 6233d8f58e1b1de2fe81dae1f6536e569b6159cb
# external library dependancies
using JSON
using DataFrames

<<<<<<< HEAD

# functions to export
export gbq_large_results, gbq_show, gbq_head, gbq_list_tables, 
gbq_list_datasets, gbq_list_projects, gbq_query, gbq_create_dataset, 
gbq_copy_table, gbq_table_exists


# custom exception for GBQ package
type GBQException <: Exception
           var::String
end
Base.showerror(io::IO, e::GBQException) = print(io, e.var);
=======
# functions to export
export gbq_large_results, gbq_show, gbq_head, gbq_list_tables, gbq_list_datasets, gbq_list_projects, gbq_query, gbq_create_dataset, gbq_copy_table
>>>>>>> 6233d8f58e1b1de2fe81dae1f6536e569b6159cb


# runs a query and saves results directly into a table
# query = the query to run
# destination = destination table (must not already exist)
<<<<<<< HEAD
# table = dataset to store table in
#
# returns DataFrame conatining a preview of the new table
function gbq_large_results(query, project, dataset, table)
  if gbq_table_exists(dataset, table)
    throw(GBQException("Destination Table Already Exists"))
  end
  response = JSON.parse(readall(`bq --format=json --quiet=True query
  --destination_table=$project:$dataset.$table "$query"`))
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
function _gbq_parse(gbq_dict)
=======
# table = data set to store table in
function gbq_large_results(query, project, dataset, table)
  println("Calling BigQuery...")
  run(`bq --format=json --quiet=True query
  --destination_table=$project:$dataset.$table "$query"`)
  return true
end


# internal function to parse results returned from big query
function _gbq_parse(gbq_dict)
  println("Parsing BigQuery Results")
>>>>>>> 6233d8f58e1b1de2fe81dae1f6536e569b6159cb
  df = DataFrame()
  cols = Dict()
  for n in collect(keys(gbq_dict[1]))
    cols[n] = []
  end
  for row in gbq_dict
      for n in collect(keys(row))
        cols[n] = [cols[n], row[n]]
      end
  end
  df = convert(DataFrame, cols)
  return df
end


# Execute a query 
<<<<<<< HEAD
#
# Returns a dataframe
function gbq_query(query, max_rows=100000)
  response = JSON.parse(readall(`bq --format=json  --quiet=True query --max_rows="$max_rows" "$query"`))
  return _gbq_parse(response)
end


# list all projects associated with default billing account
#
# Returns a dataframe
function gbq_list_projects()
  response = JSON.parse(readall(`bq ls --format=json -p`))
  return _gbq_parse(response)
end


# lists datasets in default project
#
# Returns a dataframe
function gbq_list_datasets()
  response = JSON.parse(readall(`bq ls --format=json`))
  return _gbq_parse(response)
end


# lists all tables in a specified dataset
#
# Returns a dataframe
function gbq_list_tables(dataset)
  response = JSON.parse(readall(`bq ls --format=json "$dataset"`))
  return _gbq_parse(response)
end


# preview the head of the table
#
# Returns a dataframe
function gbq_head(dataset, table, num_rows=10)
  response = JSON.parse(readall(`bq --format=json head -n $num_rows $dataset.$table`))
  return _gbq_parse(response)
=======
function gbq_query(query, max_rows=1000000)
  data = JSON.parse(readall(`bq --format=json  --quiet=True query --max_rows="$max_rows" "$query"`))
  data = _gbq_parse(data)
  return data
end


# list all projects associated with billing account
function gbq_list_projects()
  data = JSON.parse(readall(`bq ls --format=json -p`))
  return _gbq_parse(data)
end


# list off datasets in default project
function gbq_list_datasets()
  data = JSON.parse(readall(`bq ls --format=json`))
  return _gbq_parse(data)
end


# list all tables in a specified dataset
function gbq_list_tables(dataset)
  data = JSON.parse(readall(`bq ls --format=json "$dataset"`))
  return _gbq_parse(data)
end


# list all tables in a specified dataset
function gbq_head(dataset, table, num_rows=10)
  data = JSON.parse(readall(`bq --format=json head -n $num_rows $dataset.$table`))
  return _gbq_parse(data)
>>>>>>> 6233d8f58e1b1de2fe81dae1f6536e569b6159cb
end


# examine the schema for a table
<<<<<<< HEAD
#
# Returns dict containing schema
function gbq_show(dataset, table)
  return JSON.parse(readall(`bq --format=json show $dataset.$table`))
=======
function gbq_show(dataset, table)
  data = JSON.parse(readall(`bq --format=json show $dataset.$table`))
  return data
>>>>>>> 6233d8f58e1b1de2fe81dae1f6536e569b6159cb
end


# create a new dataset in the default project
<<<<<<< HEAD
#
# returns string containing response from Google BigQuery
function gbq_create_dataset(dataset)
  return readall(`bq mk $dataset`)
end


# copy a table
#
# returns string containing response from Google BigQuery
function gbq_copy_table(dataset1, table1, dataset2, table2)
  return readall(`bq cp --quiet=True $dataset1.$table1 $dataset2.$table2`)
=======
function gbq_create_dataset(dataset)
  data = readall(`bq mk $dataset`)
  return data
end

# copy a table
function gbq_copy_table(dataset1, table1, dataset2, table2)
  data = readall(`bq cp $dataset1.$table1 $dataset2.$table2`)
  return data
>>>>>>> 6233d8f58e1b1de2fe81dae1f6536e569b6159cb
end

end # module
