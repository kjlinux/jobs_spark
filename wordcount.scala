// Imports
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// Créer une session Spark
val spark = SparkSession.builder()
  .master("local[3]")
  .appName("SparkByExamples.com")
  .getOrCreate()

// Charger les données à partir d'un fichier .txt
val rdd = spark.sparkContext.textFile("/home/master/ebook.txt")

// Transformation flatMap
val rdd2 = rdd.flatMap(f => f.split(" "))

// Transformation map
val rdd3: RDD[(String, Int)] = rdd2.map(m => (m, 1))

// Transformation filter
val rdd4 = rdd3.filter(a => a._1.startsWith("a"))

// Transformation reduceByKey
val rdd5 = rdd3.reduceByKey(_ + _)

// Transformation sortByKey
val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()

// Imprimer le résultat à la console
rdd6.foreach(println)
