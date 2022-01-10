# Databricks notebook source
# Databricks notebook source
username="yuxuan_gao"
MoviePipelinePath=f"dbfs:/FileStore/Movie_final/{username}"

# COMMAND ----------

landingPath = MoviePipelinePath + "landing/"
rawPath = MoviePipelinePath + "raw/"
bronzePath = MoviePipelinePath + "bronze/"
silverPath = MoviePipelinePath + "silver/"
silverQuarantinePath = MoviePipelinePath + "silverQuarantine/"
goldPath = MoviePipelinePath + "gold/"
genreSilverPath=MoviePipelinePath + "genreSilver/"
languageSilverPath=MoviePipelinePath+"languageSilver/"

# COMMAND ----------


