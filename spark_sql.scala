import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Créer une session Spark
val spark = SparkSession.builder.appName("MonthlyPurchaseCount").getOrCreate()

// Lire le fichier en tant que DataFrame
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")

// Convertir start_date en type Date
val customerWithDate = customer.withColumn("start_date", to_date(col("start_date"), "dd/MM/yyyy"))

// Créer une vue temporaire pour exécuter des requêtes SQL
customerWithDate.createOrReplaceTempView("customer")

// Exécuter la requête SQL
val monthlyPurchaseCount = spark.sql("""
  SELECT MONTH(start_date) as month, COUNT(*) as count
  FROM customer
  GROUP BY MONTH(start_date)
""")

// Afficher les résultats
monthlyPurchaseCount.show()
