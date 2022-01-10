# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def ingest_raw_data(raw_location:str) -> DataFrame:
    return [file.path for file in dbutils.fs.ls(raw_location) if "movie_" in file.path]

# COMMAND ----------

def read_batch_raw(raw_paths: str) -> DataFrame:
    return spark.read.option("multiline", "true").format("json").load(raw_paths).select(explode("movie").alias("movies"))

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
    )

# COMMAND ----------

def read_batch_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.table("movie_bronze").filter("status = 'new'")


# COMMAND ----------

def transform_raw(rawDF: DataFrame) -> DataFrame:
    return rawDF.select(
        "movies",
    lit("local_json_files").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
    ).dropDuplicates()


# COMMAND ----------

def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    silverDF=bronzeDF.select("movies"
                             ,"movies.BackdropUrl"
                             ,"movies.Budget"
                             ,"movies.OriginalLanguage"
                             ,"movies.CreatedBy"
                             ,"movies.Id","movies.Price"
                             ,"movies.Revenue","movies.RunTime")

    if not quarantine:
        transformed_silverDF = silverDF.select(
            "movies",
            col("Id").cast("integer").alias("movie_id"),
            "Budget",
            "OriginalLanguage",
            "Revenue",
            "Price",
            col("RunTime").cast("integer")
        ).dropDuplicates()
    else:
        transformed_silverDF = silverDF.select(
            "movies",
            col("Id").cast("integer").alias("movie_id"), ##this data type changed because there's no fix needed
            "Budget", ##col("Budget").cast("integer") ## which is different from the 'not quarantine' scenario
            "OriginalLanguage",
            "Revenue",
            "Price",
            "RunTime" ##col("RunTime").cast("integer") ##which is different from the 'not quarantine' scenario
        ).dropDuplicates()
    return transformed_silverDF

# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter((dataframe.RunTime >0) & (dataframe.Budget>=1000000)),
        dataframe.filter((dataframe.RunTime<=0) | (dataframe.Budget<1000000))
    )

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.movies = dataframe.movies"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
   
    repairDF = bronzeQuarTransDF.withColumn("Budget",lit(1000000)).withColumn("RunTime",abs(bronzeQuarTransDF.RunTime))
    
    silverCleanedDF = repairDF.select(
        "movies",
        "movie_id",
        col("Budget").cast("Double"),
        "OriginalLanguage",
        "Revenue",
        "Price",
        col("RunTime").cast("integer")
    )
    return silverCleanedDF


# COMMAND ----------

def get_genre_lookup_table(
    spark: SparkSession, bronzeTable: str
) -> DataFrame:
    bronzeDF=spark.read.table(bronzeTable)
    silverGenreDF=bronzeDF.select(explode("movies.genres").alias("gen"))
    silverGenreDF=silverGenreDF.select("gen.id","gen.name").na.drop()
    silverGenreDF=silverGenreDF.dropDuplicates()
    silverGenreNotNullDF = silverGenreDF.withColumn('name', when(col('name') == '', None).otherwise(col('name'))).na.drop()
    silverGenreNotNullDF = silverGenreNotNullDF.select("id", "name" )
    return silverGenreNotNullDF

# COMMAND ----------

def get_language_lookup_table(
    spark: SparkSession, bronzeTable: str
) -> DataFrame:
    bronzeDF=spark.read.table(bronzeTable)
    silverLanguageDF=bronzeDF.select("movies.Originallanguage").na.drop().distinct()
    silverLanguageIdDF=silverLanguageDF.withColumn("id",monotonically_increasing_id())
    silverLanguageIdDF=silverLanguageIdDF.select("id", "Originallanguage")
    return silverLanguageIdDF

# COMMAND ----------


