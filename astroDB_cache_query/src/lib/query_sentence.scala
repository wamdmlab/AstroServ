package lib

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.redislabs.provider.redis._
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.immutable.HashMap
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
/**
  * Created by yc on 2017/6/6.
  */
class abnormalStarQuery(sc: SparkContext, sqlContext: SQLContext,
                        time:Array[Double], blocks:Array[(String,RDD[(String, Array[Double],Array[Row])])],
                        redisSliceNum:Int,
                        eachInterval:Int,
                        cpu:Int) {

  val timeIdx = new parseTimeIndex(sc,time,blocks,redisSliceNum)
  var tableNameDir  = List[String]()
  val abStarTableStruct =
    StructType(Array(
      StructField("star_id", StringType, true),
      StructField("ccd_num", IntegerType, true),
      StructField("imageid", IntegerType, true),
      StructField("zone", IntegerType, true),
      StructField("ra", DoubleType, true),
      StructField("dec", DoubleType, true),
      StructField("mag", DoubleType, true),
      StructField("x_pix", DoubleType, true),
      StructField("y_pix", DoubleType, true),
      StructField("ra_err", DoubleType, true),
      StructField("dec_err", DoubleType, true),
      StructField("x", DoubleType, true),
      StructField("y", DoubleType, true),
      StructField("z", DoubleType, true),
      StructField("flux", DoubleType, true),
      StructField("flux_err", DoubleType, true),
      StructField("normmag", DoubleType, true),
      StructField("flag", DoubleType, true),
      StructField("background", DoubleType, true),
      StructField("threshold", DoubleType, true),
      StructField("mag_err", DoubleType, true),
      StructField("ellipticity", DoubleType, true),
      StructField("class_star", DoubleType, true),
      StructField("orig_catid", IntegerType, true),
      StructField("timestamp", IntegerType, true)
    ))

  val magTableStruct =
    StructType(Array(
      StructField("star_id", StringType, true),
      StructField("mag", DoubleType, true),
      StructField("timestamp", IntegerType, true)
    ))

  val starNameTableStruct =
    StructType(Array(
      StructField("star_id", StringType, true)
    ))




  def countAppro(): Double = {

    val leftVal = time(0)
    val rightVal = time(1)
    if(rightVal - leftVal<=eachInterval) {
      println(s"maxTime-minTime must be more than $eachInterval")
      return -1
    }

    val blocksRDDSet = blocks.map(_._2.map(_._1))
    var ccdBlockMap = new HashMap[String, ArrayBuffer[String]]
    blocksRDDSet.flatMap(_.collect()).foreach { str =>
      val blockInfo = str.split("_")
      val CCDNo = blockInfo(1)
      val abCountName  = "abcount_"+blockInfo(1)+"_"+blockInfo(2)
      if(ccdBlockMap.contains(CCDNo))
        ccdBlockMap(CCDNo) += abCountName
      else
        ccdBlockMap += (CCDNo -> ArrayBuffer(abCountName))
    }

    var totalNum: Double = 0.0
    //countZsetMapRDD=(ccd, abcount_partition)
    val countZsetMapRDD: Map[String, RDD[(String, Double)]] = ccdBlockMap.mapValues{ turple =>
      val rddTmp =
        sc.fromRedisZRangeByScoreWithScore(turple.toArray, leftVal, rightVal,redisSliceNum)
      rddTmp
    }

    var resultEmpty = true
    countZsetMapRDD.foreach(tuple => if(!tuple._2.isEmpty()) resultEmpty=false)
    if(resultEmpty == true)
      totalNum = 0.0
    else {
      val countMapRDD = countZsetMapRDD.mapValues {
        turple =>
          val rddTmp = turple.map {
            str =>
              val tmp = str._1.split(" ")
              val T = tmp(0).toInt
              val N = tmp(1).toInt
              (T, N, str._2)
          }
          rddTmp
      }

////////////////////////使用 q4 timeInterval(-inf,+inf) pixelRound(1,2048,2048,3000) 321 2321
//测试redis中存储的abcount的zset数据是够有错
//      val countMapRDD11 = countZsetMapRDD.map {
//        turple =>
//          val rddTmp = turple._2.collect().map {
//            str =>
//              val tmp = str._1.split(" ")
//              if(StringUtils.isNumeric(tmp(0))) {
//                val T = tmp(0).toInt
//                val N = tmp(1).toInt
//                (T, N, str._2)
//              } else {
//                println("~~~~"+tmp(0)+"~~~~~")
//                (1,1,str._2)
//              }
//          }
//          turple._1 -> rddTmp
//      }
//测试redis中存储的abcount的zset数据是够有错
/////////////////////

      //该处不能使用mapValues，否则将导致mapPartitions不能序列化

        val minTimeMap = countZsetMapRDD.map {
          turple =>
               val min = turple._2.values.min()
              turple._1->min
          }

      //  val aa = countMapRDD("1").collect()

        val countPatitionMap = countMapRDD.map{
        turple:(String,RDD[(Int,Int,Double)])=>
           val minValRDD=turple._2.mapPartitions {
          input =>
            var result = List[Int]()
            var res = 0
            input.foreach(a => if (a._3 == minTimeMap(turple._1)) res += a._1 else res += a._2)
            result=result.::(res)
            result.iterator
        }
          turple._1 -> minValRDD
      }
       // val aaa = countPatition.collect()

        countPatitionMap.foreach{
          turple=> totalNum+=turple._2.sum()
        }
    }
    totalNum
  }

  def countApproWithTimeIndex(): Double = {
//    val res = timeIdx.pixelAbStarId()
    val res = timeIdx.pixelAbStarIdWithJoin()
    val starSum = res.map{t=>t.count()}.sum
    starSum
  }

  def abStarIdApproArr(): Array[String] = {
//    val res = timeIdx.pixelAbStarId_ICDE2013()
    val res = timeIdx.pixelAbStarId()
         val result = res.flatMap(_.collect())
    if(result.isEmpty) {
      println("The result set is empty.")
      return Array.empty[String]
    }
      result.foreach(println)
//   println(result.head)
      result
    }

  def abStarIdApproOutput(outputDir:String):Unit ={

//    val res = timeIdx.pixelAbStarId_ICDE2013()
    val res = timeIdx.pixelAbStarId()
     res.foreach(_.saveAsTextFile(outputDir))
      println("OK!")
  }

  private def getRandomString(length: Int): String = {
    val base = "abcdefghijklmnopqrstuvwxyz"
    val random = new Random()
    val sb = new StringBuffer()
    for (i <- 0 until length) {
      val number = random.nextInt(base.length())
      sb.append(base.charAt(number))
    }
    sb.toString
  }

  private def parseAbStarTable(starInfo:RDD[String]): RDD[Row] ={

         val res = starInfo.map { str => str.split(" ") }.map {
           p => Row(
             if (p(0) == null) null else p(0),
             if (p(1) == null) null else p(1).toInt,
             if (p(2) == null) null else p(2).toInt,
             if (p(3) == null) null else p(3).toDouble,
             if (p(4) == null) null else p(4).toDouble,
             if (p(5) == null) null else p(5).toDouble,
             if (p(6) == null) null else p(6).toDouble,
             if (p(7) == null) null else p(7).toDouble,
             if (p(8) == null) null else p(8).toDouble,
             if (p(9) == null) null else p(9).toDouble,
             if (p(10) == null) null else p(10).toDouble,
             if (p(11) == null) null else p(11).toDouble,
             if (p(12) == null) null else p(12).toDouble,
             if (p(13) == null) null else p(13).toDouble,
             if (p(14) == null) null else p(14).toDouble,
             if (p(15) == null) null else p(15).toDouble,
             if (p(16) == null) null else p(16).toDouble,
             if (p(17) == null) null else p(17).toDouble,
             if (p(18) == null) null else p(18).toDouble,
             if (p(19) == null) null else p(19).toDouble,
             if (p(20) == null) null else p(20).toDouble,
             if (p(21) == null) null else p(21).toDouble,
             if (p(22) == null) null else p(22).toDouble,
             if (p(23) == null) null else p(23).toInt,
             if (p(24) == null) null else p(24).toInt)
         }
          res
  }

  private def createAbStarTable(starInfo: RDD[String]): String = {
    val starRow =parseAbStarTable(starInfo)
    var tableName = getRandomString(12)
    //适用于多用户查询，预留接口
    while(tableNameDir.contains(tableName))
      tableName = getRandomString(12)
    tableNameDir = tableNameDir.::(tableName)
    sqlContext.createDataFrame(starRow, abStarTableStruct).registerTempTable(tableName)
    tableName
  }


  private def parseMagTable(starInfo: RDD[(Double,Array[String])]): RDD[Row] = {

    val res = starInfo.flatMap { turple => turple._2.map {
      str =>
        val p = str.split(" ")
        Row(
          if (p(0) == null) null else p(0),
          if (p(1) == null) null else p(1).toDouble,
          if (turple._1.isNaN) null else Math.floor(turple._1).toInt)
    }
    }
    res
  }

  private def createMagTable(starInfo: RDD[(Double,Array[String])]): String = {
    val starRow =parseMagTable(starInfo)
   // val aa = starRow.collect()
    var tableName = getRandomString(12)
    //适用于多用户查询，预留接口
    while(tableNameDir.contains(tableName))
      tableName = getRandomString(12)
    tableNameDir = tableNameDir.::(tableName)
    sqlContext.createDataFrame(starRow, magTableStruct).registerTempTable(tableName)
    tableName
  }


  private def parseStarNameTable(starInfo: RDD[String]): RDD[Row] = {

    val res = starInfo.map { p =>
        Row(
          if (p == null) null else p)
    }
    res
  }

  private def createStarNameTable(starInfo: RDD[String]): String = {
    val starRow =parseStarNameTable(starInfo)
       //val aa = starRow.collect()
    var tableName = getRandomString(12)
    //适用于多用户查询，预留接口
    while(tableNameDir.contains(tableName))
      tableName = getRandomString(12)
    tableNameDir = tableNameDir.::(tableName)
    sqlContext.createDataFrame(starRow, starNameTableStruct).registerTempTable(tableName)
    tableName
  }


  private def abLightCurveAppro(): (RDD[(String, Int)], DataFrame) = {
//    val starNameRDD = timeIdx.pixelAbStarId()
      val starNameRDD = timeIdx.pixelAbStarIdWithJoin()
    val starNameIter= starNameRDD.iterator
    var starNameRDDNoArr = starNameIter.next()
    while(starNameIter.hasNext)
     starNameRDDNoArr = starNameRDDNoArr.union(starNameIter.next())
    if(starNameRDDNoArr.isEmpty)
      return (sc.emptyRDD[(String,Int)],sqlContext.emptyDataFrame)

    val a = 1
    val starNameWithoutBeginTime =
      starNameRDDNoArr.map {
        id =>
          (id.substring(0, id.lastIndexOf("_")), 1)
      }.reduceByKey(_ + _)
    //取消注释实现按异常次数排序
//        .sortByKey()
   // val staraaa = starNameWithoutBeginTime.collect()
    val starInfo = sc.fromRedisZSet(starNameRDDNoArr.collect(),redisSliceNum)
    val tableName = createAbStarTable(starInfo)
    val res = sqlContext.sql(
      s"""
        |SELECT star_id, mag, timestamp
        |FROM $tableName
      """.stripMargin
      //取消注释实现按星名和时间戳排序
//        |ORDER BY star_id, timestamp
        )
    //适用于多用户查询，预留接口
    tableNameDir = tableNameDir.drop(tableNameDir.indexOf(tableName))
//    val aa = res.collect()

    (starNameWithoutBeginTime,res)
  }

  private def abLightCurveApproWithEPI(): (RDD[(String, Int)], DataFrame) = {
        val starNameRDD = timeIdx.pixelAbStarIdWithJoin_ICDE2013()
    val starNameIter= starNameRDD.iterator
    var starNameRDDNoArr = starNameIter.next()
    while(starNameIter.hasNext)
      starNameRDDNoArr = starNameRDDNoArr.union(starNameIter.next())
    if(starNameRDDNoArr.isEmpty)
      return (sc.emptyRDD[(String,Int)],sqlContext.emptyDataFrame)

    val a = 1
    val starNameWithoutBeginTime =
      starNameRDDNoArr.map {
        id =>
          (id.substring(0, id.lastIndexOf("_")), 1)
      }.reduceByKey(_ + _)
    //取消注释实现按异常次数排序
    //        .sortByKey()
    // val staraaa = starNameWithoutBeginTime.collect()
    val starInfo = sc.fromRedisZSet(starNameRDDNoArr.collect(),redisSliceNum)
    val tableName = createAbStarTable(starInfo)
    val res = sqlContext.sql(
      s"""
         |SELECT star_id, mag, timestamp
         |FROM $tableName
      """.stripMargin
      //取消注释实现按星名和时间戳排序
      //        |ORDER BY star_id, timestamp
    )
    //适用于多用户查询，预留接口
    tableNameDir = tableNameDir.drop(tableNameDir.indexOf(tableName))
    //    val aa = res.collect()

    (starNameWithoutBeginTime,res)
  }


def abLightCurveApproArr(): Unit ={

  val result = abLightCurveAppro()
  if(result._1.isEmpty()) {
    println("The result set is empty.")
//    return (Array.empty[(String, Int)], Array.empty[Row])
  }
//  val result1 = result._1.collect()
//  val result2 = result._2.collect()
//  result1.foreach(tmp => println(tmp))
//  result2.foreach(tmp => println(tmp))
  result._2.show()
// (result1,result2)
}

  def abLightCurveApproArrWithEPI(): Unit ={

    val result = abLightCurveApproWithEPI()
    if(result._1.isEmpty()) {
      println("The result set is empty.")
    }
    result._2.show()
  }


  def abLightCurveApproOutput(outputDir:String): Unit = {

    val result = abLightCurveAppro()
    if(result._1.isEmpty())
      println("The result set is empty.")
    else{
      result._1.saveAsTextFile(outputDir+"/starName")
      result._2.write.json(outputDir+"/abLightCurve")
      println("OK!")
    }


  }

def abPlusNormLightCurveAppro(): DataFrame ={

  val leftVal = time(0)
  val rightVal = time(1)
  if(rightVal - leftVal<=eachInterval) {
    println(s"maxTime-minTime must be more than $eachInterval")
    return sqlContext.emptyDataFrame
  }

//  val starNameRDD = timeIdx.pixelAbStarId_ICDE2013()
    val starNameRDD = timeIdx.pixelAbStarId()

  val starNameIter= starNameRDD.iterator
  var starNameRDDNoArr = starNameIter.next()
  while(starNameIter.hasNext)
    starNameRDDNoArr = starNameRDDNoArr.union(starNameIter.next())


  if(starNameRDDNoArr.isEmpty())
    return sqlContext.emptyDataFrame
  val starNameWithoutAbFlag = starNameRDDNoArr.map{starname =>
    starname.substring(starname.indexOf("_")+1,starname.lastIndexOf("_"))}.distinct()

 val starNameTableName = createStarNameTable(starNameWithoutAbFlag)


  val blocksRDDSet = blocks.map(_._2.map(_._1))
  val blocksArr = blocksRDDSet.flatMap(_.collect()).map {
    str =>
    val blockNo = str.split("_", 2)(1)
    val blockName = "block_" + blockNo
     blockName
  }

  val blockStrRDD = sc.fromRedisZRangeByScoreWithScore(blocksArr, leftVal,rightVal,cpu)
                 .map{starBlock =>
                 val starArr = starBlock._1.split("\n")
                   starBlock._2 -> starArr}

  val tableName = createMagTable(blockStrRDD)

  //SPark1.6.2 不支持IN或EXISTS，使用半连接LEFT SEMI JOIN实现IN或EXISTS功能
  val res = sqlContext.sql(
      s"""
         |SELECT $tableName.star_id, mag, timestamp
         |FROM $tableName
         |LEFT SEMI JOIN $starNameTableName
         |ON $tableName.star_id = $starNameTableName.star_id
      """.stripMargin
    //取消注释实现按星名排序
//         |ORDER BY $tableName.star_id, timestamp
                )

  //SPark1.6.2 不支持IN或EXISTS，使用全连接=实现IN或EXISTS功能，性能低于半连接
//  s"""
//     |SELECT $tableName.star_id, mag, timestamp
//     |FROM $tableName, $starNameTableName
//     |WHERE $tableName.star_id = $starNameTableName.star_id
//     |ORDER BY $tableName.star_id, timestamp
//        """.stripMargin
  //适用于多用户查询，预留接口
  tableNameDir = tableNameDir.drop(tableNameDir.indexOf(tableName))
  tableNameDir = tableNameDir.drop(tableNameDir.indexOf(starNameTableName))
  res
}

  def abPlusNormLightCurveApproWithOneStar(starName:String): DataFrame ={

    val leftVal = time(0)
    val rightVal = time(1)
    if(rightVal - leftVal<=eachInterval) {
      println(s"maxTime-minTime must be more than $eachInterval")
      return sqlContext.emptyDataFrame
    }

    val starNameWithoutAbFlag = starName.substring(starName.indexOf("_")+1,starName.lastIndexOf("_"))

          val blockNo = starNameWithoutAbFlag.substring(starNameWithoutAbFlag.indexOf("_")+1,
            starNameWithoutAbFlag.lastIndexOf("_"))
        val blockName = "block_" + blockNo

    val blockStrRDD = sc.fromRedisZRangeByScoreWithScore(blockName, leftVal,rightVal,cpu)
      .map{starBlock =>
        val starArr = starBlock._1.split("\n")
        starBlock._2 -> starArr}

    val tableName = createMagTable(blockStrRDD)

    val res = sqlContext.sql(
      s"""
         |SELECT $tableName.star_id, mag, timestamp
         |FROM $tableName
         |WHERE $tableName.star_id = '$starNameWithoutAbFlag'
      """.stripMargin
      //取消注释实现按星名排序
      //         |ORDER BY $tableName.star_id, timestamp
    )

    //适用于多用户查询，预留接口
    tableNameDir = tableNameDir.drop(tableNameDir.indexOf(tableName))
    res
  }
//PCSE (precise count of scientific events)
  def actualAbstarNum(spaceParam:Array[String]): DataFrame ={
    val x = spaceParam(0)
    val y=spaceParam(1)
    val r= spaceParam(2)
    val starNameRDD = timeIdx.pixelAbStarIdWithJoin()
    val starNameIter= starNameRDD.iterator
    var starNameRDDNoArr = starNameIter.next()
    while(starNameIter.hasNext)
      starNameRDDNoArr = starNameRDDNoArr.union(starNameIter.next())
    if(starNameRDDNoArr.isEmpty)
      return sqlContext.emptyDataFrame

    val a = 1
    val starNameWithoutBeginTime =
      starNameRDDNoArr.map {
        id =>
          (id.substring(0, id.lastIndexOf("_")), 1)
      }.reduceByKey(_ + _)
    //取消注释实现按异常次数排序
    //        .sortByKey()
    // val staraaa = starNameWithoutBeginTime.collect()
    val starInfo = sc.fromRedisZSet(starNameRDDNoArr.collect(),redisSliceNum)
    val tableName = createAbStarTable(starInfo)
    val res = sqlContext.sql(
      s"""
         |SELECT COUNT(star_id)
         |FROM
         |    (SELECT DISTINCT star_id, x_pix, y_pix
         |     FROM $tableName) tmp
         |WHERE SQRT((x_pix-$x)*(x_pix-$x)+(y_pix-$y)*(y_pix-$y))<=$r
      """.stripMargin
      //取消注释实现按星名和时间戳排序
      //        |ORDER BY star_id, timestamp
    )
    //适用于多用户查询，预留接口
    tableNameDir = tableNameDir.drop(tableNameDir.indexOf(tableName))
    //    val aa = res.collect()
    res
  }
  def actualAbstarNumArr(spaceParam:Array[String]): Unit ={
    val res = actualAbstarNum(spaceParam)
    if(res == sqlContext.emptyDataFrame)
      println("The count is " + res)
    else {
      println("The count is ")
      res.show()
    }

  }


  def abPlusNormLightCurveApproWithOneStarArr(starName:String): Unit ={
    val res = abPlusNormLightCurveApproWithOneStar(starName:String)
    if(res == sqlContext.emptyDataFrame) {
      println("The result set is empty.")
      Array.empty[Row]
    } else {
      res.show()
    }
  }

  def abPlusNormLightCurveApproArr():Unit={
    val res = abPlusNormLightCurveAppro()
    if(res == sqlContext.emptyDataFrame) {
      println("The result set is empty.")
      Array.empty[Row]
    }
    else {
    res.show()
//      val result = res.collect()
//      result.foreach(tmp => println(tmp))
    }
  }

  def abPlusNormLightCurveApproOutput(outputDir:String):Unit={
    val res = abPlusNormLightCurveAppro()
    if(res == sqlContext.emptyDataFrame)
      println("The result set is empty.")
    else {
      //    res.show()
      res.write.json(outputDir)
      println("OK!")
    }
  }
}