# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook : Case Study Solution
# MAGIC  * Requirement no - to display solution for every requirement
# MAGIC  * Magic commands used (sh, fs, sql)

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pandas-profiling
# MAGIC pip install ipywidgets

# COMMAND ----------

from pyspark.sql  import functions as func
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType, IntegerType
import numpy as nm
import pandas as pd
import pandas_profiling
dataloadschema = StructType(fields=[
    StructField('subtitle', StringType(), True),
    StructField('series', ArrayType(StringType(), True), True),
    StructField('latest_revision', LongType(), True),
    StructField('genres', ArrayType(StringType(), True), True),
    StructField('contributions', ArrayType(StringType(), True), True),
    StructField('contributors', ArrayType(StructType([StructField('name', StringType(), True),StructField('role', StringType(), True)]), True), True),
    StructField('revision', LongType(), True),
	StructField('title', StringType(), True),
    StructField('languages', ArrayType(StructType([StructField('key', StringType(), True)]), True), True),
    StructField('subjects', ArrayType(StringType(), True), True),
    StructField('subject_times', ArrayType(StringType(), True), True),
    StructField('subject_time', ArrayType(StringType(), True), True),
    StructField('subject_places', ArrayType(StringType(), True), True),
    StructField('subject_place', ArrayType(StringType(), True), True),
    StructField('subject_people', ArrayType(StringType(), True), True),
    StructField('source_records', ArrayType(StringType(), True), True),
    StructField('publish_country', StringType(), True),
    StructField('by_statement', StringType(), True),
	StructField('type', StructType([StructField('key', StringType(), True)]), True),
    StructField('location', ArrayType(StringType(), True), True),
    StructField('publishers', ArrayType(StringType(), True), True),
    StructField('last_modified', StructType([StructField('type', StringType(), True),StructField('value', StringType(), True)]), True),
    StructField('key', StringType(), True),
	StructField('authors', ArrayType(StructType([StructField('author', StructType([StructField('key', StringType(), True)]), True),StructField('key', StringType(), True),StructField('type', StringType(), True)]), True), True),
	StructField('publish_places', ArrayType(StringType(), True), True),
	StructField('oclc_number', ArrayType(StringType(), True), True),
    StructField('oclc_numbers', ArrayType(StringType(), True), True),
	StructField('pagination', StringType(), True),
	StructField('created', StructType([StructField('type', StringType(), True),StructField('value', StringType(), True)]), True),
	StructField('notes', StructType([StructField('type', StringType(), True),StructField('value', StringType(), True)]), True),
	StructField('number_of_pages', LongType(), True),
	StructField('publish_date', StringType(), True),
	StructField('works', ArrayType(StructType([StructField('key', StringType(), True)]), True), True),
	StructField('lc_classifications', ArrayType(StringType(), True), True),
	StructField('edition_name', StringType(), True),
	StructField('table_of_contents', ArrayType(StructType([StructField('type', StructType([StructField('key', StringType(), True)]), True),StructField('value', StringType(), True),StructField('title', StringType(), True),StructField('pagenum', StringType(), True),StructField('label', StringType(), True),StructField('class', StringType(), True),StructField('level', LongType(), True)]), True), True),
	StructField('other_titles', ArrayType(StringType(), True), True),
	StructField('dewey_decimal_class', ArrayType(StringType(), True), True),
	StructField('isbn_10', ArrayType(StringType(), True), True),
	StructField('isbn_13', ArrayType(StringType(), True), True),
	StructField('title_prefix', StringType(), True),
	StructField('physical_dimensions', StringType(), True),
    StructField('bio', StringType(), True),
    StructField('birth_date', StringType(), True),
    StructField('copyright_date', StringType(), True),
    StructField('covers', ArrayType(LongType(), True), True),
    StructField('death_date', StringType(), True),
    StructField('description', StructType([StructField('type', StringType(), True),StructField('value', StringType(), True)]), True),
    StructField('dewey_number', ArrayType(StringType(), True), True),
    StructField('download_url', ArrayType(StringType(), True), True),
    StructField('alternate_names', ArrayType(StringType(), True), True),
    StructField('excerpts', ArrayType(StructType([StructField('excerpt', StructType([StructField('type', StringType(), True),StructField('value', StringType(), True)]), True),StructField('page', StringType(), True)]), True), True),
    StructField('first_sentence', StructType([StructField('type', StringType(), True),StructField('value', StringType(), True)]), True),
     StructField('full_title', StringType(), True),
    StructField('first_publish_date', StringType(), True),
    StructField('fuller_name', StringType(), True),
    StructField('ia_box_id', ArrayType(StringType(), True), True),
    StructField('ia_loaded_id', StringType(), True),
    StructField('identifiers', StructType([StructField('goodreads', ArrayType(StringType(), True), True),StructField('amazon', ArrayType(StringType(), True), True),StructField('amazon.co.uk_asin', ArrayType(StringType(), True), True),StructField('librarything', ArrayType(StringType(), True), True)]), True),
    StructField('isbn_invalid', ArrayType(StringType(), True), True),
    StructField('isbn_odd_length', ArrayType(StringType(), True), True),
    StructField('lccn', ArrayType(StringType(), True), True),
    StructField('name', StringType(), True),
    StructField('ocaid', StringType(), True),
    StructField('personal_name', StringType(), True),
    StructField('photos', ArrayType(LongType(), True), True),
    StructField('physical_format', StringType(), True),
    StructField('purchase_url', ArrayType(StringType(), True), True),
    StructField('uri_descriptions', ArrayType(StringType(), True), True),
    StructField('uris', ArrayType(StringType(), True), True),
    StructField('url', ArrayType(StringType(), True), True),
    StructField('website', StringType(), True),
    StructField('weight', StringType(), True),
    StructField('work_title', ArrayType(StringType(), True), True),
    StructField('work_titles', ArrayType(StringType(), True), True),
    StructField('links', ArrayType(StructType([StructField('url', StringType(), True),StructField('type', StructType([StructField('key', StringType(), True)]), True),StructField('title', StringType(), True)]), True), True)
 ])
    
 

# COMMAND ----------

# MAGIC %md
# MAGIC  REQUIREMENT :1 
# MAGIC * LOAD THE DATA 
# MAGIC * PRINT SCHEMA
# MAGIC * COUNT ROWS IN RAW DATA SET

# COMMAND ----------

df_jsondataraw = spark.read.schema(dataloadschema).json("/mnt/datalkds1791/processed/ol_cdump.json")

# COMMAND ----------

df_jsondataraw.printSchema()

# COMMAND ----------

count_rows = df_jsondataraw.select("*").count()
print("COUNT OF ROWS IN RAW DATASET: ", count_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 2
# MAGIC * APPLY DATA PROFILING - pandas-profiling used
# MAGIC * generated report attached to ppt

# COMMAND ----------

df_jsondataraw_pandas = df_jsondataraw.select("*").toPandas()

# COMMAND ----------

prof = pandas_profiling.ProfileReport(df_jsondataraw_pandas.sample(n=10000))

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 3
# MAGIC 
# MAGIC * Filter out null titles , Apply no_of_pages >20 and publishing year > 1950
# MAGIC * Count rows in filtered dataset

# COMMAND ----------

filtereddata = df_jsondataraw.filter("title is not null").filter("number_of_pages is not null").filter(func.col("number_of_pages") > 20 ).filter(func.col("publish_date") > 1950)


# COMMAND ----------

count_filtereddata = filtereddata.select("*").count()
print("Count of Rows in Filtered DataSet: ", count_filtereddata)

# COMMAND ----------

filtereddata.createOrReplaceTempView("datasetCaseStudy_tempView")

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 4
# MAGIC * Get the book with the most pages.

# COMMAND ----------

mostpages = filtereddata.sort(func.col("number_of_pages").desc()).first()

# COMMAND ----------

print("Book with most pages is: ", mostpages.title, ", Number of pages: ", mostpages.number_of_pages)

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 4 
# MAGIC * Find the top 5 genres with most books

# COMMAND ----------

grouped_genres = filtereddata.groupby("genres").agg(func.count("key").alias("count_of_books"))

# COMMAND ----------

topfivegenres = grouped_genres.sort(func.col("count_of_books").desc()).filter("genres is not null").head(5)

# COMMAND ----------

print("Top five genres with most books : is displayed below:")
display(topfivegenres)

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 4
# MAGIC * Retrieve the top 5 authors who (co-)authored the most books

# COMMAND ----------

cleaneddata = filtereddata.withColumn("authors_key", func.col("authors.key"))

# COMMAND ----------

coauthored_books = cleaneddata.groupby("title").agg(func.countDistinct("authors_key").alias("count_of_authors_per_book")).filter(func.col("count_of_authors_per_book") > 1)

# COMMAND ----------

df_joined_title = coauthored_books.join(cleaneddata , "title" , "inner")

# COMMAND ----------

print("Top 5 authors who co-authored the most books is displayed below: ")
display(df_joined_title.groupby("authors_key").agg(func.countDistinct("title").alias("count_of_books")).sort(func.col("count_of_books").desc()).head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 4 
# MAGIC * Per publish year, get the number of authors that published at least one book.

# COMMAND ----------

authors_publishyear = cleaneddata.groupby("publish_date","authors_key").agg(func.countDistinct("title").alias("count_of_books")).filter(func.col("count_of_books") >= 1).groupby("publish_date").agg(func.countDistinct("authors_key").alias("count_of_authors")).sort("publish_date")


# COMMAND ----------

print(" Per publish year- Count of authors that published atleast one book is displayed below:")
display(authors_publishyear)

# COMMAND ----------

# MAGIC %md
# MAGIC REQUIREMENT : 4
# MAGIC * Find the number of authors and number of books published per year for years between 1950 and 1970

# COMMAND ----------

authors_books_publishyr = cleaneddata.filter(func.col("publish_date") < 1970).groupby("publish_date").agg(func.countDistinct("authors_key").alias("Count_of_authors"),func.countDistinct("title").alias("Count_of_books")).sort("publish_date")

# COMMAND ----------

print("Number of authors and number of books published per year between 1950 to 1970 is displayed below:")
display(authors_books_publishyr)

# COMMAND ----------


