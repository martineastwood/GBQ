using GBQ
using Base.Test

# write your own tests here
@test 1 == 1
@test gbq_query("SELECT * FROM `nyc-tlc.green.trips_2014` LIMIT 5;") 