from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, explode_outer, from_json, col, split, lit, expr
from pyspark.sql.types import StructType, StringType, MapType
from timeit import default_timer
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = "C:\Hadoop"

spark = SparkSession.builder.appName('test').config("spark.driver.memory", "10g").getOrCreate()

start = default_timer()
sdf = spark.read.format("parquet").load("C:/Users/aaa/Documents/pythondev/fiverr_job/fixed_parquet/*.parquet")
rows = sdf.count()
print(f"number of rows: {rows}")
# deleting geometry field
sdf = sdf.drop("geometry")

# flattening names field
sdf = sdf.withColumn("names_common", sdf.names.getItem("common"))
sdf = sdf.withColumn("names_official", sdf.names.getItem("official"))
sdf = sdf.withColumn("names_alternate", sdf.names.getItem("alternate"))
sdf = sdf.withColumn("names_short", sdf.names.getItem("short")).drop("names")

sdf = sdf.withColumn("names_common_exploded", explode_outer(sdf.names_common)).drop('names_common')
sdf = sdf.withColumn("names_common_value", sdf.names_common_exploded.getItem("value"))
sdf = sdf.withColumn("names_common_value", regexp_replace("names_common_value", '"', ''))
sdf = sdf.withColumn("names_common_language",sdf.names_common_exploded.getItem("language")).drop('names_common_exploded')

sdf = sdf.withColumn("names_official_exploded", explode_outer(sdf.names_official)).drop('names_official')
sdf = sdf.withColumn("names_official_value", sdf.names_official_exploded.getItem("value"))
sdf = sdf.withColumn("names_official_language",sdf.names_official_exploded.getItem("language")).drop('names_official_exploded')

sdf = sdf.withColumn("names_alternate_exploded", explode_outer(sdf.names_alternate)).drop('names_alternate')
sdf = sdf.withColumn("names_alternate_value", sdf.names_alternate_exploded.getItem("value"))
sdf = sdf.withColumn("names_alternate_language",sdf.names_alternate_exploded.getItem("language")).drop('names_alternate_exploded')

sdf = sdf.withColumn("names_short_exploded", explode_outer(sdf.names_short)).drop('names_short')
sdf = sdf.withColumn("names_short_value", sdf.names_short_exploded.getItem("value"))
sdf = sdf.withColumn("names_short_language",sdf.names_short_exploded.getItem("language")).drop('names_short_exploded')


# flattening brand field
sdf = sdf.select(sdf['*'], sdf["brand.names"].alias("brand_names"), sdf["brand.wikidata"].alias("brand_wikidata")).drop('brand')
sdf = sdf.withColumn("brand_names_common", sdf.brand_names.getItem("brand_names_common")).drop('brand_names')

sdf = sdf.withColumn("brand_names_common_exploded", explode_outer(sdf.brand_names_common)).drop('brand_names_common')
sdf = sdf.withColumn("brand_names_common_value", sdf.brand_names_common_exploded.getItem("value"))
sdf = sdf.withColumn("brand_names_common_language", sdf.brand_names_common_exploded.getItem("language")).drop('brand_names_common_exploded')

# flattening addresses field
sdf = sdf.withColumn("addresses_exploded", explode_outer(sdf.addresses)).drop('addresses')
sdf = sdf.withColumn("addresses_freeform", sdf.addresses_exploded.getItem("freeform"))
sdf = sdf.withColumn("addresses_locality", sdf.addresses_exploded.getItem("locality"))
sdf = sdf.withColumn("addresses_postcode", sdf.addresses_exploded.getItem("postcode"))
sdf = sdf.withColumn("addresses_region", sdf.addresses_exploded.getItem("region"))
sdf = sdf.withColumn("addresses_country", sdf.addresses_exploded.getItem("country")).drop('addresses_exploded')

#max_array_size = sdf.selectExpr("max(size(sources))").collect()[0][0]
max_array_size = 175
print(max_array_size)

# # # flattening sources field
# for i in range(max_array_size):
#     sdf = sdf.withColumn(f"sources[{i+1}]", col("sources")[i])
#     sdf = sdf.withColumn(f"sources[{i+1}]_properties", sdf[f"sources[{i+1}]"].getItem("properties"))
#     sdf = sdf.withColumn(f"sources[{i+1}]_dataset", sdf[f"sources[{i+1}]"].getItem("dataset"))
#     sdf = sdf.withColumn(f"sources[{i+1}]_recordid", sdf[f"sources[{i+1}]"].getItem("recordid")).drop(f'sources[{i+1}]')
sdf = sdf.drop('sources')


# flattening websites
sdf = sdf.withColumn("websites", explode_outer(sdf.websites))

# flattening socials
sdf = sdf.withColumn("socials", explode_outer(sdf.socials))

# flattening emails
sdf = sdf.withColumn("emails", explode_outer(sdf.emails))

# flattening phones
sdf = sdf.withColumn("phones", explode_outer(sdf.phones))

# flattening bbox
sdf = sdf.select(sdf['*'], 
                 sdf["bbox.minx"].alias("bbox_minx"), 
                 sdf["bbox.maxx"].alias("bbox_maxx"), 
                 sdf["bbox.miny"].alias("bbox_miny"), 
                 sdf["bbox.maxy"].alias("bbox_maxy")).drop('bbox')

# flattening categories
sdf = sdf.select(sdf['*'], sdf["categories.main"].alias("categories_main"), sdf["categories.alternate"].alias("categories_alternate")).drop('categories')
max_array_size = sdf.selectExpr("max(size(categories_alternate))").collect()[0][0]
sdf = sdf.select(sdf["*"], sdf.categories_alternate[0], sdf.categories_alternate[1]).drop("categories_alternate")

# flattening geom
sdf = sdf.withColumn("geometry",from_json(sdf.geom,MapType(StringType(),StringType()))).drop("geom")
sdf = sdf.withColumn("type", sdf.geometry.getItem("type"))
sdf = sdf.withColumn("coordinates", sdf.geometry.getItem("coordinates")).drop('geometry')
sdf = sdf.select(sdf["*"], regexp_replace(col("coordinates"), "[\[\]]", "").alias("cleaned_coordinates")).drop("coordinates")
sdf = sdf.withColumn("latitude", split(sdf["cleaned_coordinates"], ",").getItem(0))
sdf = sdf.withColumn("longitude", split(sdf["cleaned_coordinates"], ",").getItem(1)).drop('cleaned_coordinates')

rows = sdf.count()
print(f"number of rows after cleaning: {rows}")
sdf.printSchema()

sdf.filter(sdf.names_common_value == "Carmona - Parroquia san Pedro").show(truncate=False)
sdf.filter(sdf.names_common_value == "Active Printers").show(truncate=False)
#output
#sdf.write.option("header",True).option("encoding", "UTF-8").csv("c:\\Users\\aaa\\Documents\\pythondev\\fiverr_job\\testing.csv")
end = default_timer()
print(end-start)
sdf.write.option("header",False).option("encoding", "UTF-8").csv("c:\\Users\\aaa\\Documents\\pythondev\\fiverr_job\\final1.csv")