// Imports
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder()
  .master("local[3]")
  .appName("SparkByExamples.com")
  .config("spark.executor.memory", "1g")
  .config("spark.executor.cores", "1")
  .config("spark.executor.instances", "4")
  .config("spark.driver.memory", "1g")
  .config("spark.driver.cores", "1")
  .getOrCreate()

// Charger les données à partir d'un fichier .txt
val rdd = spark.sparkContext.textFile("/home/master/ebook.txt")

// Transformation flatMap
val rdd2 = rdd.flatMap(f => f.split(" "))

// Transformation map
val rdd3: RDD[(String, Int)] = rdd2.map(m => (m, 1))

// Définir la lettre de début
val startLetter = "s"

// Transformation filter
val rdd4 = rdd3.filter(a => a._1.startsWith(startLetter))

// Transformation reduceByKey
val rdd5 = rdd4.reduceByKey(_ + _)

// Transformation sortByKey
val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()

// Imprimer le résultat à la console
rdd6.foreach(println)
