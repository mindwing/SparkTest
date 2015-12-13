import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mindwing on 2015-12-13.
  */
object WordCount {
  def main(args: Array[String]) {
    println("========== WordCount")

    // workaround for 'log4.configuration' problem
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // 'bin/winutils.exe'
    System.setProperty("hadoop.home.dir", "c:\\Users\\mindwing\\warehouse\\")

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    // various approaches.
    //    val fairy = sc.textFile("file:/Users/mindwing/#dev/SparkTest/fairy_tales.txt")
    //    val fairy = sc.textFile("file:///Users/mindwing/#dev/SparkTest/fairy_tales.txt")
    //    val fairy = sc.textFile("file:/C:/Users/mindwing/#dev/SparkTest/fairy_tales.txt")
    //    val fairy = sc.textFile("file:///C:/Users/mindwing/#dev/SparkTest/fairy_tales.txt")
    //    val fairy = sc.textFile("C:\\Users\\mindwing\\#dev\\SparkTest\\fairy_tales.txt", 2).cache
    //    val fairy = sc.textFile("C:/Users/mindwing/#dev/SparkTest/fairy_tales.txt", 2).cache
    val fairy = sc.textFile("fairy_tales.txt", 2).cache

    println("fairy: " + fairy.count)

    fairy.flatMap(x => x.split(" ")).map(x => (x, 1))
      .reduceByKey((x, y) => (x + y))
      .sortBy(_._2, false)
      .take(10).map(println)
  }
}
