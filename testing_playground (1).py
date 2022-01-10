# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC 
# MAGIC %run ./operations

# COMMAND ----------

##land file to the filepath
raw_paths = [file.path for file in dbutils.fs.ls("dbfs:/FileStore/tables/") if "movie_" in file.path]

# COMMAND ----------

print(raw_paths)

# COMMAND ----------

##this can be the read_batch_raw function in operations/utilities
##this part has already explode the nested json value so no need to included in bronze transform
from pyspark.sql.functions import *
rawDF = (spark.read
         .option("multiline", "true")
         .format("json")
         .load(raw_paths)
         .select(explode("movie").alias("movies")))


# COMMAND ----------

display(rawDF)

# COMMAND ----------

##this can be the tranform_raw(rawDF) function in operations
from pyspark.sql.functions import current_timestamp, lit

transformed_rawDF = rawDF.select(
    "movies",
    lit("local_json_files").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
).dropDuplicates()

# COMMAND ----------

display(transformed_rawDF)

# COMMAND ----------

## this is the batch_writer part!! for now it is just hardcoding, details in operation section
##using overwrite over here to eliminate some problems
from pyspark.sql.functions import col

(
    transformed_rawDF.select(
        "datasource",
        "ingesttime",
        "movies",
        "ingestdate",
        "status"
    )
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

## this is the part read_batch_bronze in operations
bronzeDF=spark.read.table("movie_bronze")

# COMMAND ----------

##unpacking the nested json file
silverDF=bronzeDF.select("movies", "movies.BackdropUrl","movies.Budget","movies.OriginalLanguage","movies.CreatedBy","movies.Id","movies.Price","movies.Revenue","movies.RunTime")

# COMMAND ----------

display(silverDF)

# COMMAND ----------

##transform_bronze, operation needed,
from pyspark.sql.functions import col

transformed_silverDF = silverDF.select(
    "movies",
    col("Id").cast("integer").alias("movie_id"),
    "Budget",
    "OriginalLanguage",
    "Revenue",
    "Price",
    col("RunTime").cast("integer")
).dropDuplicates()

# COMMAND ----------

transformed_silverDF.count()

# COMMAND ----------

silver_movie_clean = transformed_silverDF.filter((transformed_silverDF.RunTime >0) & (transformed_silverDF.Budget>=1000000))
silver_movie_quarantine = transformed_silverDF.filter((transformed_silverDF.RunTime<=0) | (transformed_silverDF.Budget<1000000))

# COMMAND ----------

silver_movie_quarantine.show()

# COMMAND ----------

## write clean batch to the silver path
(
    silver_movie_clean.select(
        "movie_id", "Budget", "OriginalLanguage", "Revenue", "Price","RunTime"
    )
    .write.format("delta")
    .mode("overwrite")
    .save(silverPath)
)

# COMMAND ----------

##create sql silver table
spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_silver

# COMMAND ----------

##update the status of silver_clean into silveraugmented marked as loaded
from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_movie_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movies = clean.movies"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

##update the status of silver_quarantine marked as quarantined
silverAugmented = silver_movie_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.movies = quarantine.movies"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_bronze
# MAGIC where status = "quarantined"

# COMMAND ----------

##this part is to repair the quarantine data/1.read from bronze table that labled 'quarantined', 2 transfrom quarantined bronze data, 3. repair the transformed quarantined bronzed data 4.batch write the update data to silver table
##this is step 1
bronzeQuarantinedDF = spark.read.table("movie_bronze").filter(
    "status = 'quarantined'"
)

# COMMAND ----------

bronzeQuarantinedDF.count()

# COMMAND ----------

##step2 transform quarantined bronze data
#not changing data type in this step
#change data type in update silver step
bronzQuarTransDF=bronzeQuarantinedDF.select("movies", "movies.BackdropUrl","movies.Budget","movies.OriginalLanguage","movies.CreatedBy","movies.Id","movies.Price","movies.Revenue","movies.RunTime")
bronzQuarTransDF = bronzQuarTransDF.select(
    "movies",
    col("Id").cast("integer").alias("movie_id"), ##this data type changed because there's no fix needed
    "Budget", ##col("Budget").cast("integer") ## which is different from the 'not quarantine' scenario
    "OriginalLanguage",
    "Revenue",
    "Price",
    "RunTime"
    ##col("RunTime").cast("integer") ##which is different from the 'not quarantine' scenario
).dropDuplicates()

# COMMAND ----------

bronzQuarTransDF.count()

# COMMAND ----------

##step3 repair the quarantined table
repairDF=bronzQuarTransDF.withColumn("Budget",lit(1000000)).withColumn("RunTime",abs(bronzQuarTransDF.RunTime))

# COMMAND ----------

repairDF.show(10)

# COMMAND ----------

##step 3.1 change datatype that not changed in step2 and select to the silverRepairedDF
silverRepairedDF=repairDF.select(
    "movies",
    "movie_id",
    col("Budget").cast("Double"),
    "OriginalLanguage",
    "Revenue",
    "Price",
    col("RunTime").cast("integer")
)
silverRepairedDF.show(10)

# COMMAND ----------

##step4 update repaired data to silver table and 4.1 update status on bronze table: 'quarantined' status to 'loaded' status
(
    silverRepairedDF.select(
        "movie_id", "Budget", "OriginalLanguage", "Revenue", "Price","RunTime"
    )
    .write.format("delta")
    .mode("append")
    .save(silverPath)
)

# COMMAND ----------

##step4.1 update status on bronze table: 'quarantined' status to 'loaded' status
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverRepairedDF.withColumn("status", lit("loaded"))

update_match = "bronze.movies = clean.movies"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

display(bronzeQuarantinedDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from movie_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct OriginalLanguage from movie_silver

# COMMAND ----------

## to build a genre look up table
#1.i need to extract data from movie_bronze's "movies" column which includes metadata: movies,movie genre id, movie genre name
#2,transform this selected movie_bronze into genre_silver that eliminate null value of the movie genre name column, and select distinct genre_id
#3,create a genre lookup table and save genre data into it

## same process for a language table

# COMMAND ----------

##step1 extract data from movie_bronze table and get only genre info, transform
bronzeDF=spark.read.table("movie_bronze")
silverGenreDF=bronzeDF.select(explode("movies.genres").alias("gen"))
silverGenreDF=silverGenreDF.select("gen.id","gen.name").na.drop()
silverGenreDF=silverGenreDF.dropDuplicates()
silverGenreNotNullDF = silverGenreDF.withColumn('name', when(col('name') == '', None).otherwise(col('name'))).na.drop()

# COMMAND ----------

##step2 write batch to genresilver  path
(
    silverGenreNotNullDF.select(
        "id", "name"
    )
    .write.format("delta")
    .mode("overwrite")
    .save(genreSilverPath)
)

# COMMAND ----------

##step3 create a silver table for genre_silver
spark.sql(
    """
DROP TABLE IF EXISTS genre_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genre_silver
USING DELTA
LOCATION "{genreSilverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from genre_silver

# COMMAND ----------

##same process for language
bronzeDF=spark.read.table("movie_bronze")
silverLanguageDF=bronzeDF.select("movies.Originallanguage").na.drop().distinct()
silverLanguageIdDF=silverLanguageDF.withColumn("id",monotonically_increasing_id())

# COMMAND ----------

##batch write to languageSilverPath
(
    silverLanguageIdDF.select(
        "id", "Originallanguage"
    )
    .write.format("delta")
    .mode("append")
    .save(languageSilverPath)
)

# COMMAND ----------

##create silver table
spark.sql(
    """
DROP TABLE IF EXISTS language_silver
"""
)

spark.sql(
    f"""
CREATE TABLE language_silver
USING DELTA
LOCATION "{languageSilverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from language_silver

# COMMAND ----------


