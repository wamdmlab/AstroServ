import lib.{query_engine, read_template_table}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// the query engine is use for querying the abnormal star data from redis cluster
// we implement 4 queries as follows:
//1、查询出现异常星的个数
//2、查询异常过的星id
//3、查询异常过的星的整个异常光变曲线或时序性属性
//4、查询异常星的异常光变曲线部分+正常部分
//上述任务加时间范围和空间范围


//

// query_engine_v2 and query_template_v2 are used for querying the normal star data, and in this version
// they are unuse

object astroDB_query {

  def main(args: Array[String]): Unit = {

    if (args(0) == "-h") {
      println(
        """-outputPath: write result to the output path with json,
          |and "show" will send result to the master node
          |and show them in screen"""".stripMargin)
      println("-sliceNum: the rdd patition number")
      println("-redisHost: ip:port")
      println("-ccdNumDefault: the default CCD number")
      println("-eachInterval: the default sample period of CCD")
//      println("-allPartitionNum: the number of partitions assigned in insertion compoment")
//      println("-needPartitionNum: the required number of partitions filtered out by query engine, " +
//                                 "if allPartitionNum=needPartitionNum, the filter won't be executed")
      println("-cpu: the number of cpu cores over the cluster")
      System.exit(1)
    }



    var outputPath = new String
    var sliceNum = 0
    var queryName = new String
    var queryParam = new String
    var redisHost = new String
    var ccdNumDefault = 0
    var  eachInterval = 0
    var cpu = 0
//    var allPartitionNum = 0
//    var needPartitionNum = 0
    for(a <- args.indices)
      {
        if (args(a).head == '-')
          {
            args(a) match {
              case "-outputPath"    => outputPath = args(a+1)
              case "-sliceNum"  => sliceNum = args(a+1).toInt
              case "-redisHost" => redisHost = args(a+1)
              case "-ccdNumDefault" => ccdNumDefault = args(a+1).toInt
              case "-eachInterval" => eachInterval = args(a+1).toInt
              case "-cpu" => cpu = args(a+1).toInt
//              case "-allPartitionNum" => allPartitionNum=args(a+1).toInt
//              case "-needPartitionNum" => needPartitionNum=args(a+1).toInt
              case _ => sys.error(s"${args(a)} is illegal")
            }
          }
      }

    val redisIP = redisHost.split(":")(0)
    val redisPort = redisHost.split(":")(1)

    val conf = new SparkConf()
      .setAppName(s"AstroDB_cache_query")
      .set("spark.scheduler.mode", "FAIR")
      .set("redis.host", redisIP)
      .set("redis.port", redisPort)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[RDD[String]],classOf[RDD[Row]]))
//      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val spaceIdxCache = new read_template_table(sc,sqlContext,cpu)
                        .fromRedis(sliceNum,ccdNumDefault)
    // test code
    //val sp = spaceIdxCache("spaceIdx_1").coalesce(8,true).collect()
    //val a = spaceIdxCache._1.collect()

    val qEngine = new query_engine(sc,spaceIdxCache,sqlContext,sliceNum,eachInterval,cpu)
    println("please input query name and parameters")
    var line  = Console.readLine
    var outputDirMap = new mutable.HashMap[String,Int]
    while (line != "exit") {
     time {
       var outputDir = outputPath
       val lineParam = line.split(" ", 2)
       val queryName = lineParam(0)
       val queryParam = lineParam(1).split(" ")
       if (outputDir != "show") {
         if (!outputDirMap.contains(queryName))
           outputDirMap += (queryName -> 1)
         else
           outputDirMap(queryName) += 1
         outputDir = outputDir + "/" + queryName + "_" + outputDirMap(queryName)
       }

       qEngine.runUserQuery(queryName, queryParam, outputDir)
       println("please input query name and parameters")
     }
      line = Console.readLine
    }
 }
  def time[R](block: => R): R = {
    val start = System.nanoTime()
    val result = block // call-by-name
    val end = System.nanoTime()
    println("\n\n--------------------------------")
    println("past:[" + (end - start)/1000000000d + "s]")
    println("--------------------------------")
    result
  }
}
