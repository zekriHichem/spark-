import org.apache.spark._
import org.apache.spark.SparkContext._
object WordCount {
    def main(args: Array[String]) {  
      val input =  sc.textFile("text_ex1.txt")
      val words = input.flatMap(line => line.split(" "))
      val counts =words.map(word =>(word, 1)).reduceByKey{case (x, y)=> x + y}
      counts.saveAsTextFile("rscala")
    }
}
