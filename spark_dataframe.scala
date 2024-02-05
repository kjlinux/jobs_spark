import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Créer une session Spark
val spark = SparkSession.builder.appName("CustomerOrderCount").getOrCreate()

// Lire les fichiers en tant que DataFrames
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")
val orderDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/order.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")
val order = orderDF.toDF("#id", "total")

// Convertir start_date en type Date
val customerWithDate = customer.withColumn("start_date", to_date(col("start_date"), "dd/MM/yyyy"))

// Compter le nombre de commandes par mois
val customerMonthCount = customerWithDate.groupBy(month(col("start_date")).alias("month")).count()

// Afficher les résultats
customerMonthCount.show()
