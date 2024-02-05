import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder.appName("CustomerOrder").getOrCreate()

// Lire le fichier en tant que DataFrame
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")

// Trier les enregistrements clients par nom
val sortedCustomer = customer.orderBy("nom")

// Afficher les résultats
sortedCustomer.show()
