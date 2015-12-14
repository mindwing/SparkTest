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

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
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

    fairy.flatMap(x => x.split(" ")) // 빈 칸을 기준으로 낱말별로 나눔.
      .map(x => (x, 1)) // (x, 1) 형태의 tuple 을 가지는 PairRDD 로 만듦.
      .reduceByKey((x, y) => x + y) // 같은 key 를 가지는 value 중 두 개를 입력받아서 그 둘을 하나로 합침.
      .sortBy(_._2, false) // tuple 중에서 _2 를 기준으로 역방향 정렬한다.
      .take(10).map(println) // 순서상 맨 앞의 10개를 가져와서 println 한다.
  }
}
