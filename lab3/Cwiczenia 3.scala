// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val DF1 = namesDf.withColumn("epoch_time", unix_timestamp())

DF1.explain()
display(DF1)
DF1.printSchema()

// COMMAND ----------

val DF2 = DF1.withColumn("height_in_feet", $"height"/30.48)

DF2.explain()
display(DF2)

// COMMAND ----------

//najpoluparniejsze imie
val names = DF2.select(split($"birth_name", " ").getItem(0)).withColumnRenamed("split(birth_name,  , -1)[0]", "first_name")
val grouped_names = names.groupBy("first_name").count().orderBy($"count".desc)

display(grouped_names)

// COMMAND ----------

//liczenie wieku
val DF3 = DF2.withColumn("birth_date", to_date($"date_of_birth", "dd.MM.yyyy"))
          .withColumn("death_date", to_date($"date_of_death", "dd.MM.yyyy"))

val DF4 = DF3.withColumn("age", floor(datediff($"death_date", $"birth_date")/(365.25)))

display(DF4)

// COMMAND ----------

//usuwanie kolumn
val DF5 = DF4.drop("bio", "death_details")
display(DF5)

// COMMAND ----------

//zmiana nazw kolumn 
val DF6 = DF5.withColumnRenamed("imdb_name_id","imdbNameId")
           .withColumnRenamed("birth_name", "birthName")
           .withColumnRenamed("birth_details", "birthDetails")
           .withColumnRenamed("date_of_birth", "dateOfBirth")
           .withColumnRenamed("place_of_birth", "placeOfBirth")
           .withColumnRenamed("date_of_death", "dateOfDeath")
           .withColumnRenamed("place_of_death", "placeOfDeath")
           .withColumnRenamed("reason_of_death", "reasonOfDeath")
           .withColumnRenamed("spouses_string", "spousesString")
           .withColumnRenamed("epoch_time", "epochTime")
           .withColumnRenamed("height_in_feet", "heightInFeet")
           .withColumnRenamed("birth_date", "birthDate")
           .withColumnRenamed("death_date", "deathDate")

display(DF6)

// COMMAND ----------

val DF7 = DF6.sort("name")

display(DF7)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

val moviesDF1 = moviesDf.withColumn("epoch_time", unix_timestamp())

display(moviesDF1)

// COMMAND ----------

val moviesDF2 = moviesDF1.withColumn("movie_age", year(current_date())-$"year")

display(moviesDF2)

// COMMAND ----------

val moviesDF3 = moviesDF2.withColumn("numeric_budget", split($"budget", " ").getItem(1))

display(moviesDF3)

// COMMAND ----------

val moviesDF4 = moviesDF3.na.drop("any")

display(moviesDF4)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

// COMMAND ----------

val ratingsDF1 = ratingsDf.withColumn("epoch_time", unix_timestamp())

// COMMAND ----------

val ratingsDF2 = ratingsDF1.na.drop("any")

display(ratingsDF2)

// COMMAND ----------

val votes = ratingsDF2.select("females_allages_avg_vote", "males_allages_avg_vote")
                      .agg(avg("females_allages_avg_vote") as "female", avg("males_allages_avg_vote") as "male")

display(votes)

// COMMAND ----------

import org.apache.spark.sql.types.LongType

val ratingsDF3 = ratingsDF2.withColumn("weighted_average_vote", $"weighted_average_vote".cast(LongType))

display(ratingsDF3)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Zad2

// COMMAND ----------

/*
Jobs Tab - wyświetla informcja o wszystkich job'ach oraz podsumownaie ih wszystkich 
Stages Tab - informacje o obecnym statusie job'ów
Storage Tab - wyświetla zapisane RDD'y i DataFrame'y
Environment Tab - wyświetla informacje o różnych zmiennych środowiskowych i konfiguracyjnych 
Executors Tab - wyświetla podsumowanie o executorach, kóre zostały utworzone dla aplikacji
SQL Tab - jeśli aplikacja wykonuje zapytania Spark SQL, SQL tab wyświetla informacje o zapytaniach 
Structured Streaming Tab - wyświetla krótkie statystyki o wykonywanych i wykonanych zapytaniach 
Streaming Tab - wyświetla planowane opóźnienie i czas przetwarzania dla każdej mikro-patrii danych 
JDBC/ODBC Server Tab - wyświtla informacje o sesji i złożonych operacjach SQL'a
*/

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Zad3

// COMMAND ----------

//DF2.select("height").explain()
DF2.select("height").groupBy("height").count().explain()


// COMMAND ----------

// MAGIC %md 
// MAGIC ## Zad4

// COMMAND ----------

val jdbcDF = (spark.read.format("jdbc")
              .option("url",  "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
              .option("dbtable", "(SELECT table_name FROM information_schema.tables) tmp")
              .option("user", "sqladmin")
              .option("password", "$3bFHs56&o123$") 
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
              .load())

display(jdbcDF)
