module GBQ

# external library dependancies
using JSON
using DataFrames

# functions to export
export gbq_large_results, gbq_show, gbq_head, gbq_list_tables, gbq_list_datasets, gbq_list_projects, gbq_query, gbq_create_dataset, gbq_copy_table


# runs a query and saves results directly into a table
# query = the query to run
# destination = destination table (must not already exist)
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
end


# examine the schema for a table
function gbq_show(dataset, table)
  data = JSON.parse(readall(`bq --format=json show $dataset.$table`))
  return data
end


# create a new dataset in the default project
function gbq_create_dataset(dataset)
  data = readall(`bq mk $dataset`)
  return data
end

# copy a table
function gbq_copy_table(dataset1, table1, dataset2, table2)
  data = readall(`bq cp $dataset1.$table1 $dataset2.$table2`)
  return data
end

end # module
