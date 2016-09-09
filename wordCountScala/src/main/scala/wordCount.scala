import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AKorzilova on 09.09.2016.
  */
object wordCount {

  var inputFile: String = "C:\\Users\\AKorzilova\\Desktop\\inputFile.txt"

  var outputFile: String = "C:\\Users\\AKorzilova\\Desktop\\outputFile.txt"

  def main(args: Array[String]): Unit = {
    //create sparkContext
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    //download the source data
    val input = sc.textFile(inputFile)
    //split words
    val words = input.flatMap(line => line.split("\t"))
    //Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x+y}
    //save file
    counts.saveAsTextFile(outputFile)
  }
}
