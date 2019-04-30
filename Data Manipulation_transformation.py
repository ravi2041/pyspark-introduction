from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import udf


# Schema is defined for daily table with date as datetype and all columns based on their data value they hold. 
schema_daily = StructType([
    StructField("station_id", StringType(), True),
    StructField("date", DateType(), True),
    StructField("element", StringType(), True),
    StructField("date_value", DoubleType(), True),
    StructField("measurement_flag", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("source_flag", StringType(), True),
    StructField("observation_time", TimestampType(), True),
])

# We have passed path of file name and dateformat to read date columns and timestampformat to read observation column. 
daily = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .option("dateFormat", "YYYYMMDD") \
    .option("timestampFormat", "HHMM") \
    .load('hdfs:///data/ghcnd/daily', schema = schema_daily)


# 
stations = spark.read.text("hdfs:///data/ghcnd/stations")
stations_new = stations.select(
    stations.value.substr(1,11).cast('string').alias('station_id'),
    stations.value.substr(13,8).cast('double').alias('latitude'),
    stations.value.substr(22,9).cast('double').alias('longitude'),
    stations.value.substr(32,5).cast('double').alias('elevation'),
    stations.value.substr(39,2).cast('string').alias('state_code'),
    stations.value.substr(42,29).cast('string').alias('name'),
    stations.value.substr(73,3).cast('string').alias('gsn_flag'),
    stations.value.substr(77,3).cast('string').alias('hcn_crn_flag'),
    stations.value.substr(81,5).cast('string').alias('wmo_id')
)



countries = spark.read.text("hdfs:///data/ghcnd/countries")
countries_new = countries.select(
    countries.value.substr(1,2).cast('string').alias('country_code'),
    countries.value.substr(4,47).cast('string').alias('country_name')
)

countries_new.cache()
countries_new.show()

states = spark.read.text("hdfs:///data/ghcnd/states")
states_new = states.select(
    states.value.substr(1,2).cast('string').alias('state_code'),
    states.value.substr(4,46).cast('string').alias('state_name')
)
states_new.cache()
states_new.show()

inventory = spark.read.text("hdfs:///data/ghcnd/inventory")
inventory_new = inventory.select(
    inventory.value.substr(1,11).cast('string').alias('station_id'),
    inventory.value.substr(13,8).cast('double').alias('latitude'),
    inventory.value.substr(22,9).cast('double').alias('longitude'),
    inventory.value.substr(32,4).cast('string').alias('element'),
    inventory.value.substr(37,4).cast('integer').alias('firstyear'),
    inventory.value.substr(42,4).cast('integer').alias('lastyear')
)
inventory_new.cache()
inventory_new.show()

# To count number of rows in inventory
inventory_new.count()
#595699

# To count number of rows in stations
stations_new.count()
#103656
# To count number of rows in country
countries_new.count()
#218
# To count number of rows in states 
states_new.count()
#74

# count of stations who doesnt have wmo id
stations_new.filter(col("wmo_id") == '     ').count()
#95594

# question 3a-- added new column by taking first two string of station id  which represents country code.
# Using withcolumn() function we added new column named as country_code in stations_new table 
stations_new = stations_new.withColumn("country_code",stations_new.station_id.substr(1,2))



#  Left Join countries table to stations table on country code.
stations_new = stations_new.join(countries_new, on =["country_code"], how ="left")

#  left joined states to stations table on state code
stations_new = stations_new.join(states_new, on =["state_code"], how ="left")


# Q3d grouped by station id to get unique value for each station start year and end year. Passed aggregate function min and max to collect
# start year and end year 
first_last = inventory_new.groupby("station_id").agg({"firstyear":"min","lastyear":"max"}).withColumnRenamed("min(firstyear)","startyear")

# joined first and last year of each station to stations table. Second line is just changing column name
stations_new = stations_new.join(first_last, on = "station_id", how = "left")
stations_new = stations_new.withColumnRenamed("max(lastyear)", "endyear")


# different elements each station collected overall. Grouped data and counted elements for each station id and named it totalelements
total_elements = inventory_new.groupBy("station_id").agg({"element":"count"}).withColumnRenamed("count(element)","totalelements")

# joined total elements table to stations table
stations_new= stations_new.join(total_elements, on= "station_id", how= "left")

#Q joined inventory table annd dropped latitude and longitude as stations table already has both columns.
stations_new = stations_new.join(inventory_new.drop('latitude','longitude'), on="station_id", how="left")

# stations collecting all five core elements. using filter condition to find all stations who collect core elements and grouped by station id
core_elements = inventory_new.filter(inventory_new.element.isin("PRCP","SNOW","SNWD","TMAX","TMIN")).groupby("station_id").agg({"station_id":"count"})
.withColumnRenamed("count(station_id)", "core_five_elements")
# total count is 20224

# joined core elements table to stations table
stations_new = stations_new.join(core_elements, on = "station_id", how = "left")
# created new column  showing stations collecting all five core elements. using f.when condition to fill y for station collecting
# all core elements and no for else
stations_new = stations_new.withColumn("has_5_cores", F.when(stations_new.core_five_elements ==5, "Y")\
	.otherwise("N"))


# made udf function which returns 1 to new colun prcp_only if true and later we count the prcp collecting stations
def prcp(total, element):
	if total == 1 and element == 'PRCP' :
		return 1

prcp_udf = udf(lambda z,y: prcp(z,y), IntegerType())

stations_new = stations_new.withColumn('prcp_only', \
                prcp_udf(stations_new['totalelements'],stations_new['element']))

stations_new.filter(stations_new.prcp_only == 1).count()
# Out[48]: 15970


# collecting other elements by subtracting total elements to core elements to find counts of core elements and other elements each station
# collected
stations_new = stations_new.withColumn("Other_Elements", \
	stations_new.totalelements - stations_new.core_five_elements)


# collected all elements in an array using collect set elements. grouped by station id 
grouped_element = inventory_new.groupby("station_id") \
.agg(F.collect_set("element")) \
.withColumnRenamed("collect_set(element)","element_set")
stations_new = stations_new.join(grouped_element, on="station_id", how="left")



# joining daily 1000 rows with stations new using left join on station id
daily_subset = daily.join(stations_new, on="station_id", how="left")



# to find stations id by subtracting both distinct tables which will give station id which is not present in other
daily_stations = daily_subset.select("station_id").distinct()
stations_station_id = stations_new.select("station_id").distinct()
left_out_ids = stations_station_id - daily_stations
#Out[54]: 0

# Saving output file in the directory 

stations_new.write.format('com.databricks.spark.csv').save('hdfs:///user/rks55/outputs/ghcnd/stations_new.csv',header = 'true')
stations_new.count()
#Out[55]: 595725


Analysis


# How many stations are there in total? How many stations have been active in 2017?
# grouped all station and taken count of distinct station id 
stations_new.groupBy("station_id").agg({"station_id":"count"}).count()
#Out[60]: 103656

# filtered stations table with last year equal to 2017 and grouped by station to get count of number of stations
stations_new.select("station_id","lastyear").filter(stations_new.lastyear == 2017).groupby("station_id").agg({"station_id":"count"}).count()
#Out[118]: 37546


# calculated distinct station id and grouped by hsn flag and hcn crn flag to calculate count. We get combination as well as individual 
# counts for all flag types.
stations_dist = stations_new.select("station_id","gsn_flag","hcn_crn_flag").distinct()
stations_dist.groupby("gsn_flag", "hcn_crn_flag").agg({"station_id":"count"}).distinct().show()
# there are 14 station which are in GSN and HCN

# selected distinct country name and station id and further grouped by to get count of stations present in each country and added column
# to existing country meta table

new = stations_new.select("country_name","station_id").distinct().groupby("country_name") \
.agg({"country_name":"count"}) \
.withColumnRenamed("count(country_name","count_of_stations")
# joined above output in countries new table 
countries_new = countries_new.join(new, on="country_name", how="left")


# same procedure as above but instead of countries we calculated stations count in each state and joined the column to states meta table
new_states = stations_new.select("state_name","station_id").distinct().groupby("state_name") \
.agg({"state_name":"count"}) \
.withColumnRenamed("count(state_name","count_of_stations_state")

states_new = states_new.join(new_states, on="state_name", how="left")


# fitered on latitude column which is less than zero as it represents southern hemisphere. later grouped by station ids and calculated count
stations_new.select("station_id", "latitude","country_name").filter(stations_new.latitude < 0).groupby("station_id").agg({"station_id":"count"}).count()
#Out[75]: 25337


# searched using regular expression and grouped the entire result by station id and found the total count using station id
stations_new.where('country_name rlike "(United States)"').groupby("station_id").agg({"station_id":"count"}).count()
#Out[79]: 57227




# Cross joined stations table with itself and changed latiutde and longitude name so that we can calculate  distance respect to other
# stations
new_df = stations_new.withColumnRenamed("latitude","latitude2").crossJoin(stations_new.withColumnRenamed("longitude","longitude2")).limit(100)

# udf function is created to calculate haversine distance between two stations using haversine formulae
from math import radians, cos, sin, asin, sqrt, atan2, pi

def haversine_distance(lat1, lon1, lat2, lon2):

    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """

    deg2rad = pi/180.0

    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = (lon2 - lon1) 
    dlat = (lat2 - lat1) 

    a = sin(dlat/2.0)**2.0 + cos(lat1) * cos(lat2) * sin(dlon/2.0)**2.0
    c = 2.0 * asin(sqrt(a))

    #c = 2.0 * atan2(sqrt(a), sqrt(1.0-a))

    r = 6372.8 # Radius of earth in kilometers. Use 3956 for miles #No rounding R = 3959.87433 (miles), 6372.8(km)

    return c * r

# initialised udf function and output is expected in float type 
haversine_distance_udf = udf(haversine_distance, FloatType())

# udf function is called where latitude and longitude is passed as function parameters to haversine function
new_df = new_df.withColumn('dist_km', \
                haversine_distance_udf(new_df['latitude2'],new_df['longitude']\
                    ,new_df['latitude'],new_df['longitude2'])\
                )

#new_df.show()

#Q2 b Apply this function to compute the pairwise distances between all stations in New Zealand,
#and save the result to your output directory.
#What two stations are the geographically closest in New Zealand?

# first extracted all stations which belongs to new zealand using where condition and regualar expression 
stations_new_zealand = stations_new.where('country_name rlike "(New Zealand)"')

#stations_new.where('country_name rlike "(New Zealand)"').count()
# Out[156]: 71 there are total 71 stations in new zealand

# Renamed columns names so that when we save output, we dont have to face issues of duplicated columns
stations_new_zealand_updated = stations_new_zealand.withColumnRenamed("country_name","country_name_to") \
.withColumnRenamed("station_id","station_id_to") \
.withColumnRenamed("name","name_to")

# cross joined stations in new zealand with latitude and longitude renamed so we dont get into similar column names error
new_df = stations_new_zealand.withColumnRenamed("latitude","latitude2").crossJoin(stations_new_zealand_updated.withColumnRenamed("longitude","longitude2"))

# called haversine function to calculate distance between each station in new zealand
new_df =new_df.withColumn('dist_km', \
                haversine_distance_udf(new_df['latitude2'],new_df['longitude']\
                    ,new_df['latitude'],new_df['longitude2'])\
                )
# Filtered stations

# 50.43 is the smallest distance measured. 
new_df = new_df.select("station_id","station_id_to","dist_km","name","name_to","latitude","longitude2","latitude2","longitude","country_name")
new_df.filter(new_df.dist_km >0.0).sort(asc("dist_km")).show()
new_df.write.csv("hdfs:///user/rks55/outputs/ghcnd/nz_station_distance", header = 'true')



#Q3 - A(analysis)

daily_2010 = sqlContext.read \
.format('com.databricks.spark.csv') \
.option("dateFormat", "YYYYMMDD") \
.option("timestampFormat", "HHMM") \
.load('hdfs:///data/ghcnd/daily/2010.csv.gz', schema = schema_daily)

daily_2010.count()
#Out[78]: 36946080



daily_2017= sqlContext.read \
.format('com.databricks.spark.csv') \
.option("dateFormat", "YYYYMMDD") \
.option("timestampFormat", "HHMM") \
.load('hdfs:///data/ghcnd/daily/2017.csv.gz', schema = schema_daily)
daily_2017.count()
#32190499


# used regular expression for reading file from 2010 to 2015. 
#Q 3(c) Load and count the number of rows in daily from 2010 to 2015.

daily_2010_2015 = sqlContext.read \
.format('com.databricks.spark.csv') \
.option("dateFormat", "YYYYMMDD") \
.option("timestampFormat", "HHMM") \
.load('hdfs:///data/ghcnd/daily/201[0-5].csv.gz', schema = schema_daily)

daily_2010_2015.count()
# 207716098


#Count the number of rows in daily?
daily.count()
#A.	2624027105

#Q 4 b filtered core elements from daily using isin matching expression
core_elements = daily.filter(daily.element.isin('SNOW','PRCP','SNWD','TMIN','TMAX'))

# grouped by element and counted each element.
core_elements.groupby("element").agg({"element":"count"}).show()

# +-------+--------------+
# |element|count(element)|
# +-------+--------------+
# |   TMAX|     362528096|
# |   SNOW|     322003304|
# |   SNWD|     261455306|
# |   TMIN|     360656728|
# |   PRCP|     918490401|
# +-------+--------------+

# Q4B Which element has the most observations ?? 
#PRCP has the most observations. 918490401



# filtered tmin from daily 

tmin = daily.filter(col("element") == 'TMIN').withColumnRenamed("element" , "tmin_element")
# filtered tmax from daily 
tmax = daily.filter(col("element") == 'TMAX').withColumnRenamed("element", "tmax_element")

# joined tmin and tmax on station id and date
tmin_tmax = tmin.join(tmax, on = ['station_id','date'], how= 'left')

#  we will caluclate where tmax is null and it will take only tmin 
tmin_tmax.filter(col("tmax_element").isNull()).count()
#

daily.filter(col("element")=='TMAX').groupby("station_id","date").agg({"element":"count"}).show()

tmin_tmax_data = daily.filter(daily.element.isin("TMIN","TMAX")).groupby("station_id","date").agg(F.collect_set("element")) \
.withColumnRenamed("collect_set(element)","element_set")
updated_data = tmin_tmax_data.withColumn(
  "check",
  array_contains(col("element_set"), "TMAX")
)

new_table = updated_data.filter(updated_data.check =="false")
new_table.count()
# Out[9]: 273368



hcn_check = new_table.join(stations_new, on= 'station_id', how= 'left')

hcn_check.groupby("gsn_flag","hcn_crn_flag").agg({"gsn_flag":"count","hcn_crn_flag":"count"}).show()


#Q4 d Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand,
#and save the result to your output directory.
#How many observations are there, and how many years are covered by the observations?
#(taken only pairs)
# created new column by pulling string out of 
daily_new = daily.withColumn("country_code", daily.station_id.substr(1,2))
daily_new_nz = daily_new.filter(daily_new.country_code =="NZ")
daily_new_nz_tmin_tmax = daily_new_nz.filter(daily_new_nz.element.isin("TMIN","TMAX"))
daily_new_nz_tmin_tmax.count()
daily_new_nz_tmin_tmax.write.csv("hdfs:///user/rks55/outputs/ghcnd/tmin_tmax_nz.csv", header='true')

# Output - 447017

# added year to daily tmin tmax table  and casted it to integer type so that we can calculate minimum and maximum value of year oberavtion.
daily_new_nz_tmin_tmax = daily_new_nz_tmin_tmax.withColumn("year",daily_new_nz_tmin_tmax.date.substr(1,4))
year_distinct = daily_new_nz_tmin_tmax.select(daily_new_nz_tmin_tmax.year.cast("integer")).distinct()
year_distinct.agg({"year_distinct.year":"min","year_distinct.year":"max"}).show()

# plot for time series combining columns 

daily_new_nz_tmin = daily_new_nz_tmin_tmax.filter(daily_new_nz_tmin_tmax.element =="TMIN").groupBy("year","station_id").agg({"date_value":"avg"}).withColumnRenamed("avg(date_value)","average_tmin")
daily_new_nz_tmax = daily_new_nz_tmin_tmax.filter(daily_new_nz_tmin_tmax.element =="TMAX").groupBy("year","station_id").agg({"date_value":"avg"}).withColumnRenamed("avg(date_value)","average_tmax")

daily_new_nz_tmin_average_tmin = daily_new_nz_tmin_tmax.filter(daily_new_nz_tmin_tmax.element == "TMIN").groupby("year").agg({"date_value":"avg"}).withColumnRenamed("avg(date_value)","average_tmin_for_all_stations")
daily_new_nz_tmax_average_tmax = daily_new_nz_tmin_tmax.filter(daily_new_nz_tmin_tmax.element == "TMAX").groupby("year").agg({"date_value":"avg"}).withColumnRenamed("avg(date_value)","average_tmax_for_all_stations")
daily_new_nz_tmin_tmax_value = daily_new_nz_tmax.join(daily_new_nz_tmin, on = ["station_id","year"], how = "left")
daily_new_nz_tmin_tmax_value_average = daily_new_nz_tmin_tmax_value.join(daily_new_nz_tmin_average_tmin, on = "year", how = "left")
daily_new_nz_tmin_tmax_average_plot = daily_new_nz_tmin_tmax_value_average.join(daily_new_nz_tmax_average_tmax, on = "year", how = "left")
daily_new_nz_tmin_tmax_average_plot.write.csv("hdfs:///user/rks55/outputs/ghcnd/time_series_data", header = 'true')



#Q4 e Group the precipitation observations by year and country. Compute the average rainfall
#in each year for each country, and save this result to your output directory.
#Which country has the highest average rainfall in a single year across the entire dataset?

# took country code from daily and later joined daily with countries table on country code to get country name
daily = daily.withColumn("country_code", daily.station_id.substr(1,2))

daily = daily.join(countries_new, on =["country_code"], how ="left")
# year is substringed from date column so that we can aggregate with year in later command
daily = daily.withColumn("year", daily.date.substr(1,4))


# element prcp is filtered from daily set and new dataframe is grouped by year and country name so that we can calcuate average rainfall
# for each country each year.
annual_rainfall = daily.filter(col("element").isin("PRCP")).groupBy("year","country_name") \
.agg({"date_value":"avg"}).withColumnRenamed("avg(date_value)", "average_rainfall")


# saving output in hdfs 
annual_rainfall.read.csv("hdfs:///user/rks55/outputs/ghcnd/annual_rainfall", header = 'true')


#Q Which country has the highest average rainfall in a single year across the entire dataset?
#   First average rainfall is casted to double datatype from string type. later max value of average rainfall is taken.
annual_rainfall = annual_rainfall.select(col("year"),col("country_name"),annual_rainfall.average_rainfall.cast('double').alias('average_rainfall'))

# method 1
annual_rainfall.agg({"average_rainfall":"max"}).show()

#method 2
annual_rainfall.orderBy(["average_rainfall"],ascending=[0]).show()

#|year|        country_name|  average_rainfall|
#+----+--------------------+------------------+
#|2000|   Equatorial Guinea|            4361.0|
#|1975|  Dominican Republic|            3414.0|
#|1974|                Laos|            2480.5|



# calculating cumalative rainfall per country by filtering PRCP and grouping by country name as average
cumalative_rainfall = daily.filter(col("element").isin("PRCP")).groupby("country_name").agg({"date_value":"avg"})
cumalative_rainfall = cumalative_rainfall.withColumnRenamed("avg(date_value)", "average_rainfall")
cumalative_rainfall.write.csv("hdfs:///user/rks55/outputs/ghcnd/cumalative_rainfall", header = 'true')


#----------------------------------------------------------------------------------------------------------

# Challenge first quetion: seperated core elements from the other elements by creating udf function and assigning core to core elements
# and other to other elements. Then filtered out core elements. New data set contains other elements. We grouped by country to find the 
# count of elements each country produces. 
def other_element(element):
	if element == "PRCP" or element == "SNOW" or element == 'SNWD' or element == 'TMIN' or element == 'TMAX':
		return "core"
	else:
		return "other"

other_element_udf = udf(lambda x: other_element(x), StringType())

daily_new = daily.withColumn("core_vs_other",other_element_udf(daily['element']))
daily_new = daily_new.filter(col("core_vs_other").isin("other"))
daily_new = daily_new.withColumn("country_code", daily.station_id.substr(1,2))

daily_new = daily_new.join(countries_new, on =["country_code"], how ="left")

daily_new_country = daily_new.groupby("country_name").agg({"element":"count"}).withColumnRenamed("count(element)", "element_count")
daily_new_country.orderBy(["element_count"],ascending=[0]).show()

# Temporally
daily_new = daily_new.withColumn("year", daily.date.substr(1,4))
daily_new_year = daily_new.groupby("year").agg({"element":"count"}).withColumnRenamed("count(element)", "other_element_count")
daily_new_year.show()


# Challenge Second Question
daily.select("quality_flag").distinct().show()

#+------------+
#|quality_flag|
#+------------+
#|        null|
#|           I|
#|           D|
#|           X|
#|           S|
#|           K|
#|           G|
#+------------+
quality_passed_daily = daily.filter(col("quality_flag").isNull())
quality_passed_daily.count()

#How many rows in daily failed each of the quality assurance checks?
:# 2624027105 -2616269594 = 7757511

quality_issues = daily.filter(col("quality_flag").isNotNull())
quality_issues.count()