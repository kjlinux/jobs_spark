import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Créer une session Spark
val spark = SparkSession.builder.appName("AverageOrderAmountPerCustomer").getOrCreate()

// Lire les fichiers en tant que DataFrames
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")
val orderDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/order.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")
val order = orderDF.toDF("id", "total") // Renommer la colonne "#id" en "id"

// Créer des vues temporaires pour exécuter des requêtes SQL
customer.createOrReplaceTempView("customer")
order.createOrReplaceTempView("order")

// Exécuter la requête SQL
val avgOrderAmountPerCustomer = spark.sql("""
  SELECT c.nom, AVG(o.total) as average_total
  FROM customer c, order o
  WHERE c.id = o.id
  GROUP BY c.nom
""")

// Afficher les résultats
avgOrderAmountPerCustomer.show()
