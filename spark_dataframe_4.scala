import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Créer une session Spark
val spark = SparkSession.builder.appName("AverageOrderAmountPerCustomer").getOrCreate()

// Lire les fichiers en tant que DataFrames
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")
val orderDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/order.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")
val order = orderDF.toDF("#id", "total")

// Faire une jointure entre les DataFrames customer et order sur la colonne 'id'
val joinedDF = customer.join(order, customer("id") === order("#id"))

// Calculer le montant moyen des commandes par client
val avgOrderAmountPerCustomer = joinedDF.groupBy("nom").agg(avg("total").alias("average_total"))

// Afficher les résultats
avgOrderAmountPerCustomer.show()
