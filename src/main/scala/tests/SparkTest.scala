package tests

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD;
/**
 * @author lewis
 */
object test extends App {
  println("test");
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
//  // sc is an existing SparkContext.
//  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//  
//  // Create an RDD
//  val people = sc.textFile("../../spark-1.3.1-bin-hadoop2.6/examples/src/main/resources/people.txt")
//  
//  // The schema is encoded in a string
//  val schemaString = "name age"
//  
//  // Import Row.
//  import org.apache.spark.sql.Row;
//  
//  // Import Spark SQL data types
//  import org.apache.spark.sql.types.{StructType,StructField,StringType};
//  
//  // Generate the schema based on the string of schema
//  val schema =
//    StructType(
//      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//  
//  // Convert records of the RDD (people) to Rows.
//  val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
//  
//  // Apply the schema to the RDD.
//  val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
//  
//  // Register the DataFrames as a table.
//  peopleDataFrame.registerTempTable("people")
//  
//  // SQL statements can be run by using the sql methods provided by sqlContext.
//  val results = sqlContext.sql("SELECT name FROM people")
//  
//  // The results of SQL queries are DataFrames and support all the normal RDD operations.
//  // The columns of a row in the result can be accessed by ordinal.
//  results.map(t => "Name: " + t(0)).collect().foreach(println)
  
//  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//  
//  // A JSON dataset is pointed to by path.
//  // The path can be either a single text file or a directory storing text files.
//  val path = "../../spark-1.3.1-bin-hadoop2.6/examples/src/main/resources/people.json"
//  // Create a DataFrame from the file(s) pointed to by path
//  val people = sqlContext.jsonFile(path)
//  
//  // The inferred schema can be visualized using the printSchema() method.
//  people.printSchema()
//  // root
//  //  |-- age: integer (nullable = true)
//  //  |-- name: string (nullable = true)
//  
//  // Register this DataFrame as a table.
//  people.registerTempTable("people")
  

//  val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
//  teenagers.show()
//  // Alternatively, a DataFrame can be created for a JSON dataset represented by
//  // an RDD[String] storing one JSON object per string.
//  val anotherPeopleRDD = sc.parallelize(
//    """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
//  val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
    
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._
  
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class Person(name: String, age: Int)
  
  // Create an RDD of Person objects and register it as a table.
  val people:RDD[Person] = sc.textFile("../../spark-1.3.1-bin-hadoop2.6/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
  val peoples = people.toDF().registerTempTable("people")
  //peoples.registerTempTable("people")
  
  // SQL statements can be run by using the sql methods provided by sqlContext.
  val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
  
  // The results of SQL queries are DataFrames and support all the normal RDD operations.
  // The columns of a row in the result can be accessed by ordinal.
  teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  
  
  
  
  
  
}