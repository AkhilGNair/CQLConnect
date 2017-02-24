package SparkHello

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._


object HelloWorld {

  val conf = new SparkConf()

  def sum( a:Array[Array[Int]] ) : String = {
    "Hello, world! - From Scala. Your number is " + addInt(a(0)(0), a(0)(1))
  }

  def addInt( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b

    return sum
  }

}

// Type mapping from R to Java
//
// NULL -> void
// integer -> Int
// character -> String
// logical -> Boolean
// double, numeric -> Double
// raw -> Array[Byte]
// Date -> Date
// POSIXlt/POSIXct -> Time
//
// list[T] -> Array[T], where T is one of above mentioned types
// environment -> Map[String, T], where T is a native type
// jobj -> Object, where jobj is an object created in the backend
