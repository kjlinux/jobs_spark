import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder.appName("CustomerOrder").getOrCreate()

// Lire le fichier en tant que DataFrame
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")

// Créer une vue temporaire pour exécuter des requêtes SQL
customer.createOrReplaceTempView("customer")

// Exécuter la requête SQL
val sortedCustomer = spark.sql("""
  SELECT *
  FROM customer
  ORDER BY nom
""")

// Afficher les résultats
sortedCustomer.show()
