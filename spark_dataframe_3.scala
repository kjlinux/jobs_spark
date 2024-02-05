import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder.appName("CustomerOrderJoin").getOrCreate()

// Lire les fichiers en tant que DataFrames
val customerDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/customer.txt")
val orderDF = spark.read.option("inferSchema", "true").option("header", "true").csv("file:/home/master/order.txt")

// Renommer les colonnes pour correspondre à votre schéma
val customer = customerDF.toDF("id", "start_date", "nom")
val order = orderDF.toDF("#id", "total")

// Faire une jointure entre les DataFrames customer et order sur la colonne 'id'
val joinedDF = customer.join(order, customer("id") === order("#id"))

// Sélectionner les colonnes 'nom' et 'total'
val resultDF = joinedDF.select("nom", "total")

// Afficher les résultats
resultDF.show()
