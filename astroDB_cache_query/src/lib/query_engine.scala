package lib

import java.io.File
import javax.ws.rs.QueryParam

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.redislabs.provider.redis._
import org.apache.spark.sql.types._

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap}


class query_engine(sc: SparkContext, spaceIdx:(RDD[(String,Array[Double])],
                                     immutable.HashMap[String,RDD[(String, Array[Double],Array[Row])]]),
                   sqlContext: SQLContext,sliceNum:Int,
                   eachInterval:Int,
                   cpu:Int) {

  private def createModHashQueryFromTemplate(queryName:String, queryParam: String): DataFrame = {
    //////////////////////////////////模板表原子查询
    //qt1 按星等查询：查询星等值在某个范围内[magMin,magMax]的所有目标，返回星id（只需要模板表）
    //    参数列表 ： magMin magMax
    //qt2 按位置区域查询：按赤经、赤纬，一定的搜索半径来查询该区域内的目标,返回星id（只需要模板表）
    //    参数列表: ra dec searchRadius
    //qt3 查询某颗星的ra和dec（只需要模板表）
    //    参数列表: star_id
    //qt4 查询某视场的星ID 只需要模板表
    //    参数列表: ccd_num
    ///////////////////////////////模板表组合查询
    //qt5 按目标ID所在区域查询该ID对应的目标的周围一定半径内的星id（只需要模板表）
    //    参数列表：star_id searchRadius
    /////////////////////////////////////////////



    //////////////////////////////////
val queryPattern = "(qt[01234])".r  //模板原子查询匹配表 正则表达式 https://www.iteblog.com/archives/1245.html
var templateResult = sqlContext.emptyDataFrame
    queryName match {
      case queryPattern(_) =>
        val queryTuple = (new query_template).getQuery(queryName, queryParam)
            templateResult = sqlContext.sql(queryTuple)
//        val a = templateResult.map(_.getString(0)).collect()   // used for testing q1-q4
//        a
      case "qt5" =>
        val paramArr = queryParam.split(" ")
        val queryTuple1 = (new query_template).getQuery("qt3", paramArr(0))
        val raAndDec = sqlContext.sql(queryTuple1).map {
          row =>
            val ra = row.getDouble(0)
            val dec = row.getDouble(1)
            s"$ra $dec"
        }.collect().mkString
        //raAndDec
        val queryTuple2 = (new query_template).getQuery("qt2", s"$raAndDec ${paramArr(1)}")
        templateResult = sqlContext.sql(queryTuple2)
//              val a = templateResult.map(_.getString(0)).collect()   // used for testing q5
//              a
        }
    templateResult
    }


  private def createModHashQueryFromHDFS(queryName:String, queryParam: String, starTablePath: String):
  DataFrame = {
    //////////////////////////////////原始表原子查询
    //qo1 查询某个星集合（星集合必须在一个星表簇中）中所有星的在某个时间范围的光变曲线返回光变曲线和时间戳(按星id和时间排序，只需要原始表)
    //    参数列表 ：timeMin timeMax starBacket star_id_set
    //qo2 查询某个星集合（星集合必须在一个星表簇中）中所有星在给定的时间范围都被哪些CCD拍过（检测CCD或星是否漂移），并统计不同CCD的时间戳的个数（只需要原始表)
    //    参数列表: timeMin timeMax starBacket star_id_set
    val queryTuple = (new query_template).getQuery(queryName, queryParam)
    val templateResult = sqlContext.sql(queryTuple)
    templateResult
    }

  private def userQuery(queryName:String, queryParam: String,sliceNum:Int,fromSource:String,
                starTablePath: String,backetsNum:Int): DataFrame = {
                                     //不同的hash函数这里的数值不同
    //////////////////////////////////用户查询
    //q0 输出全部的模板表
    //
    //q1 按星等查询：查询星等值在某个范围内[magMin,magMax]的所有目标，返回星id（只需要模板表）
    //    参数列表 ： magMin magMax  1 10
    //q2 按位置区域查询：按赤经、赤纬，一定的搜索半径来查询该区域内的目标,返回星id（只需要模板表）
    //    参数列表: ra dec searchRadius
    //q3 查询某颗星的ra和dec（只需要模板表）
    //    参数列表: star_id
    //q4 查询某视场的星ID 只需要模板表
    //     参数列表: ccd_num
    //q5 按目标ID所在区域查询该ID对应的目标的周围一定半径内的星id（只需要模板表）
    //    参数列表：star_id searchRadius
    //q6 查询某个星集合在某个时间范围的光变曲线返回光变曲线和时间戳(只需要原始表)
    //   参数列表：star_id_set timeMin timeMax  [ref_1_1 ref_1_11 ref_2_1],36588,36988 需要有逗号
    //q7 查询某个星集合所有星在给定的时间范围都被哪些CCD拍过（只需要原始表）
    //    参数列表：star_id_set, timeMin timeMax  [ref_1_1 ref_1_11 ref_2_1],36588,36988 需要有逗号
    //q8 按位置区域查询：按赤经、赤纬，一定的搜索半径和一定时间范围来查询该区域内的所有目标的光变曲线和时间戳（需要模板表和原始表）
    //   参数列表：ra dec searchRadius timeMin timeMax
    //q9 按目标ID所在区域查询该ID对应的目标的周围一定半径和一定时间范围内的星的光变曲线和时间戳（需要模板表和原始表）
    //   参数列表：star_id searchRadius timeMin timeMax
    //////////////////////////////////////////////////////////////
    var result = sqlContext.emptyDataFrame
    queryName match {
      case "q0" =>
       // result = createModHashQueryFromTemplate("qt0", queryParam)
        result = qoRun(queryParam,sliceNum,fromSource,starTablePath,backetsNum,"qo0")
      case "q1" =>
        result = createModHashQueryFromTemplate("qt1", queryParam)
      case "q2" =>
        result = createModHashQueryFromTemplate("qt2", queryParam)
      case "q3" =>
        result = createModHashQueryFromTemplate("qt3", queryParam)
      case "q4" =>
        result = createModHashQueryFromTemplate("qt4", queryParam)
      case "q5" =>
        result = createModHashQueryFromTemplate("qt5", queryParam)
      case "q6" =>
        result = qoRun(queryParam,sliceNum,fromSource,starTablePath,backetsNum,"qo1")
      case "q7" =>
        result = qoRun(queryParam,sliceNum,fromSource,starTablePath,backetsNum,"qo2")
      case "q8" =>
        result = qt_qoRun(queryParam,sliceNum,fromSource,starTablePath,backetsNum,"qo1")
    }
    result
  }
///////////////////////////////////////////////////////main funtion
  def runUserQuery(queryName:String, queryParam: String,sliceNum:Int,outputPath:String,
                   fromSource:String,starTablePath: String,backetsNum:Int = 0): Unit ={
//    userQuery(queryName, queryParam,sliceNum,fromSource,starTablePath,backetsNum).show()
    userQuery(queryName, queryParam,sliceNum,fromSource,starTablePath,backetsNum).write.format("com.databricks.spark.csv").save("home.csv")

  }
  ///////////////////////////////////////////////////////main funtion

  private def qoRun(queryParam: String,sliceNum:Int,fromSource:String,
                    starTablePath: String,backetsNum:Int = 0,qoX:String): DataFrame =
  {
    var result = sqlContext.emptyDataFrame
    var unionCount = 0

//    val twoPart = queryParam.split(",")
//    val starName = twoPart(0).split(" ")
//    var time = twoPart(1).split(" ")
//    if(time(0).isEmpty)
//        time=time.drop(1)   //drop the space behind "," if it exists
//
//    val timeMin = time(0).trim()
//    val timeMax = time(1).trim()

    val str_list=queryParam.split(",")
    val starName_pre=str_list(0).trim().replaceAll("\\[|\\]","").split(" ")
    val starName=starName_pre.map(i=>i.trim())

    val timeMin = str_list(1).trim()
    val timeMax = str_list(2).trim()

    if(fromSource == "HDFS") {
      val starHash = mapStarCluster(starName, backetsNum)  //CCD   //backet name  //star name

      starHash.foreach {
        ccdTuple => //CCD1
          ccdTuple._2.foreach {
            BlucketAndStarList =>
              val starCluster = s"ccd_${ccdTuple._1}_backet_${BlucketAndStarList._1}" //ccd_1_backet_5: 1号CCD的第五个桶
              val qo1_param = s"$starCluster ${BlucketAndStarList._2} $timeMin $timeMax"
              /////////////////////////////////serial///////////////////////////////
              val queryTuple = (new query_template).getQuery(qoX, qo1_param)
                                          //CCD        //backet name
              readFromHDFS(starTablePath, ccdTuple._1, BlucketAndStarList._1)
              val starClusterResult = sqlContext.sql(queryTuple)
              if(unionCount == 0){
                  result = starClusterResult
                  unionCount = 1
                }
              else{
                result = result.unionAll(starClusterResult) //不适用于并行
              }
            /////////////////////////////////serial///////////////////////////////
          }
      }
    }
    else if(fromSource == "redis")
    {
      val tableName = "redisTable"
      var starNameString = new String
      starName.foreach(str =>starNameString=starNameString+s"""'$str',""")
      starNameString=starNameString.dropRight(1)
      val qo1_param = s"$tableName $starNameString $timeMin $timeMax"
      val queryTuple = (new query_template).getQuery(qoX, qo1_param)
      readFromRedis(starTablePath,starName,sliceNum,tableName)
      val starClusterResult = sqlContext.sql(queryTuple)
      if(unionCount == 0) {
        result = starClusterResult
        unionCount = 1
      }
      else
        {
          result = result.unionAll(starClusterResult)   //不适用于并行
        }
      }
//    val a = result.collect()   //used for testing q6
    result
  }

  private def qt_qoRun(queryParam: String,sliceNum:Int,fromSource:String,
                    starTablePath: String,backetsNum:Int = 0,qoX:String): DataFrame =
  { //在qoRun中区分hdfs和redis读取星表簇
    var result = sqlContext.emptyDataFrame
    var unionCount = 0

    //----q2----//
    val paramArr = queryParam.split(" ")
    val param_q2=paramArr(0)+" "+paramArr(1)+" "+paramArr(2)
    val queryTuple1 = (new query_template).getQuery("qt2", param_q2)
    var q2_result = sqlContext.sql(queryTuple1).collect().mkString

//    println(q2_result) //[ref_1_100][ref_2_30][ref_3_20]
    val starName=q2_result.replaceAll("\\]\\["," ")
    println(starName)
    //----q6-----//
//    val param_q6=starName+","+paramArr(3)+","+paramArr(4)
    val param_q6="[ref_1_1 ref_1_11 ref_2_1],36588,36988"

    println("simulate the para for q6:"+param_q6)

    result=qoRun(param_q6,sliceNum,fromSource,starTablePath,backetsNum,"qo1")
    result
  }




  private def mapStarCluster(starName: Array[String], backetsNum:Int = 0): HashMap[String,HashMap[String,String]] = //?
  {                           //CCD       //backet name  //star name
    val starHash = new HashMap[String,HashMap[String ,  String]]
    (0 until starName.length).foreach
    {
      i =>
        val tmp = starName(i).split("_") //星名star_1_6666: tmp(1)表示的是CCD标号;tmp(2)是星的二级ID

        val backet = (tmp(2).toInt % backetsNum).toString //根据具体的哈希函数 此处需要更改

        val starListInit =s"""'${starName(i)}'"""
        if(!starHash.contains(tmp(1))) {
          val ccdBacket = new HashMap[String, String]
          ccdBacket += (backet -> starListInit)
          starHash += (tmp(1) -> ccdBacket)
        }
        else
        {
          if(!starHash(tmp(1)).contains(backet))
          {
            starHash(tmp(1))+=(backet -> starListInit)
          }
          else
          {
            val starList =s""",'${starName(i)}'"""
            starHash(tmp(1))(backet) = starHash(tmp(1))(backet) + starList
          }
        }
    }
    starHash
  }

  private def readFromHDFS(starTablePath:String, ccdID:String,backetName:String): Unit =
  {
    sqlContext.read.load(s"$starTablePath/$ccdID/$backetName").registerTempTable(s"ccd_${ccdID}_backet_$backetName")
  }


  private def readFromRedis(starTablePath:String,starName:Array[String],sliceNum:Int,tableName:String): Unit ={
    val listRDDRow = sc.fromRedisList(starName,sliceNum).map(str=>str.split(" "))
      .map{ p=> //p is one line
        Row(
          if(p(0)==null) null else p(0),
          if(p(1)==null) null else p(1).toInt,
          if(p(2)==null) null else p(2).toInt,
          if(p(3)==null) null else p(3).toInt,
          if(p(4)==null) null else p(4).toDouble,
          if(p(5)==null) null else p(5).toDouble,
          if(p(6)==null) null else p(6).toDouble,
          if(p(7)==null) null else p(7).toDouble,
          if(p(8)==null) null else p(8).toDouble,
          if(p(9)==null) null else p(9).toDouble,
          if(p(10)==null) null else p(10).toDouble,
          if(p(11)==null) null else p(11).toDouble,
          if(p(12)==null) null else p(12).toDouble,
          if(p(13)==null) null else p(13).toDouble,
          if(p(14)==null) null else p(14).toDouble,
          if(p(15)==null) null else p(15).toDouble,
          if(p(16)==null) null else p(16).toDouble,
          if(p(17)==null) null else p(17).toDouble,
          if(p(18)==null) null else p(18).toDouble,
          if(p(19)==null) null else p(19).toDouble,
          if(p(20)==null) null else p(20).toDouble,
          if(p(21)==null) null else p(21).toDouble,
          if(p(22)==null) null else p(22).toDouble,
          if(p(23)==null) null else p(23).toInt,
          if(p(24)==null) null else p(24).toInt)
      }
    val tableStruct =
      StructType(Array(
        StructField("star_id",StringType,true),
        StructField("ccd_num",IntegerType,true),
        StructField("imageid",IntegerType,true),
        StructField("zone",IntegerType,true),
        StructField("ra",DoubleType,true),
        StructField("dec",DoubleType,true),
        StructField("mag",DoubleType,true),
        StructField("x_pix",DoubleType,true),
        StructField("y_pix",DoubleType,true),
        StructField("ra_err",DoubleType,true),
        StructField("dec_err",DoubleType,true),
        StructField("x",DoubleType,true),
        StructField("y",DoubleType,true),
        StructField("z",DoubleType,true),
        StructField("flux",DoubleType,true),
        StructField("flux_err",DoubleType,true),
        StructField("normmag",DoubleType,true),
        StructField("flag",DoubleType,true),
        StructField("background",DoubleType,true),
        StructField("threshold",DoubleType,true),
        StructField("mag_err",DoubleType,true),
        StructField("ellipticity",DoubleType,true),
        StructField("class_star",DoubleType,true),
        StructField("orig_catid",IntegerType,true),
        StructField("timestamp",IntegerType,true)
      ))
    sqlContext.createDataFrame(listRDDRow,tableStruct).registerTempTable(tableName)
  }




private def parseQueryParam(queryParam:Array[String],
                            spaceIdx:(RDD[(String,Array[Double])],
                              immutable.HashMap[String,RDD[(String, Array[Double],Array[Row])]]),
                            queryName:String):
                              (Array[Double],Array[(String,RDD[(String, Array[Double],Array[Row])])],
                                Array[String],immutable.HashMap[String, Array[String]]) = {

  val timeIntervalParamRex = """timeInterval\((.*)\,(.*)\)""".r
  val pixelRoundParamRex = """pixelRound\((.*)\,(.*)\,(.*)\)""".r
  val floatRex = """(^[1-9]\d*\.\d*|0\.\d*[1-9]\d*$)""".r
  val intRex = """(^[1-9]\d*|0$)""".r

  var TS_paramMap = new immutable.HashMap[String, Array[String]]
  val atomQueryParam = new ArrayBuffer[String]
  queryParam.foreach {
    qp => qp match {
      case timeIntervalParamRex(min, max) =>
        if (min.isEmpty || max.isEmpty) {
          sys.error("in timeInterval function, some parameters are none")
        }
        TS_paramMap = TS_paramMap + ("timeInterval" -> Array(min, max))
      case pixelRoundParamRex(x, y, r) =>
        if (x.isEmpty || y.isEmpty || r.isEmpty) {
          sys.error("in pixelRound function, some parameters are none")
        }
        TS_paramMap = TS_paramMap + ("pixelRound" -> Array(x, y, r))
      case _ => atomQueryParam += qp
    }
  }

  //  val aaa = TS_paramMap
  //  val  aaaaa = 1
  TS_paramMap.foreach { param =>
    param._2.foreach { p => p match {
      case "+inf" =>
      case "-inf" =>
      case floatRex(str) =>
      case intRex(str) =>
      case _ => sys.error(s" data type error in some parameters of ${param._1} function")

    }
    }
  }

  var blocks = new ArrayBuffer[(String, RDD[(String, Array[Double], Array[Row])])]
  var timeinterval = new Array[Double](2)

  if (queryName != "q6") {
    //process the space index
    if (TS_paramMap.contains("pixelRound")) {
      val getBlock = new parseSpaceIndex(spaceIdx)
      blocks ++= getBlock.pixelRound(TS_paramMap("pixelRound"))
    } else {
      //don't provide any space index function
      val allblocks: Array[(String, RDD[(String, Array[Double], Array[Row])])] = spaceIdx._2.toArray
      blocks = blocks.union(allblocks)
    }
  }
  if (TS_paramMap.contains("timeInterval")) {
    val tmp = TS_paramMap("timeInterval")

    for (i <- timeinterval.indices) {
      if (tmp(i) == "-inf")
        timeinterval(i) = Double.NegativeInfinity
      else if (tmp(i) == "+inf")
        timeinterval(i) = Double.PositiveInfinity
      else
        timeinterval(i) = tmp(i).toDouble
    }
  } else {
    timeinterval(0) = Double.NegativeInfinity
    timeinterval(1) = Double.PositiveInfinity
  }

  (timeinterval,blocks.toArray,atomQueryParam.toArray,TS_paramMap)
  }

def runUserQuery(queryName:String, queryParam:Array[String],outputPath:String): Unit = {

  val tmp = parseQueryParam(queryParam, spaceIdx,queryName)
  val timeInterval = tmp._1
  val blocksBySpaceIdx = tmp._2
  val spaceParam = tmp._4("pixelRound")
//  val aa= blocksBySpaceIdx.collect()
  val abQuery = new abnormalStarQuery(sc, sqlContext, timeInterval, blocksBySpaceIdx, sliceNum, eachInterval,cpu)

  if (outputPath == "show") {
    queryName match {
        // probing analysis with PCAG
      case "q1" => val count = abQuery.countAppro()
        println("The count is " + count)
      case "q2" => abQuery.abStarIdApproArr()
      // listing analysis with SEPI
      case "q3" => abQuery.abLightCurveApproArr()
      case "q4" => abQuery.abPlusNormLightCurveApproArr()
      // probing analysis with SEPI
      case "q5" => val count = abQuery.countApproWithTimeIndex()
        println("The count is " + count)
        //默认tmp._3是查询参数，q6第一个参数是星id
        //stretching analysis
      case "q6" => abQuery.abPlusNormLightCurveApproWithOneStarArr(tmp._3(0))
        //listing analysis with PCSE
      case "q7" => abQuery.actualAbstarNumArr(spaceParam)
      // listing analysis with EPI
      case "q8" => abQuery.abLightCurveApproArrWithEPI()
      case _ => sys.error(s"$queryName not exists!")
    }
  } else {
    queryName match {
      case "q1" => val count = abQuery.countAppro()
        println("The count is " + count)
      case "q2" => abQuery.abStarIdApproOutput(outputPath)
      case "q3" => abQuery.abLightCurveApproOutput(outputPath)
      case "q4" => abQuery.abPlusNormLightCurveApproOutput(outputPath)
      case _ => sys.error(s"$queryName not exists!")

    }
  }
}
}
