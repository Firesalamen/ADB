# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./operations

# COMMAND ----------

####Solve negative runtime / minimum budget / duplicate movies
raw_location="dbfs:/FileStore/tables/"
raw_paths=ingest_raw_data(raw_location)

#from raw to bronze
transformedRawDF = transform_raw(rawDF)
rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, mode="overwrite")
rawToBronzeWriter.save(bronzePath)

#from bronze to silver
bronzeDF = read_batch_bronze(spark)
transformedBronzeDF = transform_bronze(bronzeDF)
(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(
    transformedBronzeDF)#quarantine

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanDF,  exclude_columns=["movies"], mode="overwrite")
bronzeToSilverWriter.save(silverPath)#write clean

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")#status update

silverCleanedDF = repair_quarantined_records(
    spark, bronzeTable="movie_bronze")
bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanedDF, exclude_columns=["movies"])
bronzeToSilverWriter.save(silverPath)#write quarantined

update_bronze_table_status(spark, bronzePath, silverCleanedDF, "loaded")#status update

####To create genre lookup table
genreDF=get_genre_lookup_table(
    spark, bronzeTable="movie_bronze")
bronzeToGenreWriter = batch_writer(
    dataframe=genreDF, mode="overwrite")
bronzeToGenreWriter.save(genreSilverPath)

####To greate language lookup table
languageDF=get_language_lookup_table(
    spark, bronzeTable="movie_bronze")
bronzeToLanguageWriter = batch_writer(
    dataframe=languageDF, mode="overwrite")
bronzeToLanguageWriter.save(languageSilverPath)
