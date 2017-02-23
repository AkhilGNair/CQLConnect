package SparkHello

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object HelloWorld {
  
  val conf = new SparkConf()
  
  def hello( a:Int ) : String = {
    "Hello, world! - From Scala. Your number is " + a
  }
  
  def addInt( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b

    return sum
  }
  
}
