package lib

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import com.redislabs.provider.redis._

import scala.collection.immutable
import scala.io.Source


//tmp 中 template1-9 代表了9个CCD 上的模板星表。一个星表17万行数据，代表了17万颗星上固定的记录，比如坐标
class read_template_table(sc : SparkContext, sqlContext: SQLContext,cpu:Int) {

  def fromLocal(tempTablePath : String,sliceNum:Int): Unit =
  {
    var tempParal= sc.parallelize(Seq[String](""))
    var i = 0
    new File(tempTablePath).listFiles().foreach{
      f => //依次读取9个文件
        val temp = Source.fromFile(f).getLines.toArray.map {
          line =>
            val arr = line.split(' ')               //原始template表 中第一列是ccd 标号 倒数第二列是这个ccd下的星号 最后一列是时间戳
            var star = s"ref_${arr(0)}_${arr(22)}" //把原始template表第一列和倒数第二列 拼在一起形成starid
            for (i <- arr.indices)
              if (i != 1 && i != 22 && i != 23) //ignore imageid(原始template表第二列 图片ID), orig_catid(倒数第二列) and timestamp(最后一列)
                star += s" ${arr(i)}"
            star
        }
        if (i == 0) {  // the first time cannot use "union" funtion because the inited "tempParal" has the space line
          tempParal = sc.parallelize(temp, sliceNum)
          i = 1
        }
        else
         tempParal=tempParal.union(sc.parallelize(temp,sliceNum)) //把9个小模板表 组合成一张大的模板表
        }

    tempParal.coalesce(sliceNum,false)

    trans_template_into_dataframeTable(tempParal)
  }

 private def trans_template_into_dataframeTable(tempParal : RDD[String]): Unit =
  {
    val tableStruct =
    StructType(Array(
    StructField("star_id",StringType,true),
    StructField("ccd_num",IntegerType,true),
    StructField("zone",IntegerType,true),
    StructField("ra",DoubleType,true),
    StructField("dec",DoubleType,true),
    StructField("mag",DoubleType,true), //
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
    StructField("class_star",DoubleType,true)
    ))

    val storeRDDRow=tempParal.map(str=>str.split(" "))
    .map{ p=> //p is one line
    Row(
    if(p(0)==null) null else p(0),
    if(p(1)==null) null else p(1).toInt,
    if(p(2)==null) null else p(2).toInt,
    if(p(3)==null) null else p(3).toDouble,
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
    if(p(21)==null) null else p(21).toDouble)
  }

    sqlContext.createDataFrame(storeRDDRow,tableStruct).cache().registerTempTable("template")
//    val b = sqlContext.sql("select * from template").count()
}
  ////////////////////////////////////////////////////////////////////////

  def fromRedis(redisSliceNum:Int,ccdNum:Int):
  (RDD[(String, Array[Double])], immutable.HashMap[String,RDD[(String, Array[Double],Array[Row])]])= {
//      val partitionMultiple = allPartitionNum/needPartitionNum
      val keypattern = "ccd_*"
        println("The template keypattern :"+keypattern)

    val ccdReginRDD = sc.fromRedisKV(keypattern,ccdNum).map{
      tuple =>
        // tuple._2.split(" ",3) 头两个是 RA DEC,不需要删除
      val regionArr = tuple._2.split(" ",3)(2).split(" ").map(_.toDouble)
        ("spaceIdx_"+tuple._1.split("_")(1), regionArr)
    }
    val keyArr = ccdReginRDD.collect().map(_._1)

    var blockReginRDD = new immutable.HashMap[String,RDD[(String, Array[Double],Array[Row])]]

    var i=0
    keyArr.foreach{
     ccdKey =>
        val spaceIdx = ccdKey
       val spaceIdxRDD = sc.fromRedisKV(spaceIdx+"_*",redisSliceNum)
//       val aa = spaceIdxRDD.collect()
        var blockMap = spaceIdxRDD.map{
          block =>
            //////////////用于过滤部分掉部分分区,在测试RATE性能约束时用到(测试无效), 当partitionMultiple为1时，不过滤
//            val blockNo = block._1.split("_")(2).toInt+1
//            var filterFlag = false
//            if(blockNo% partitionMultiple!= 1)
//              filterFlag = true
            //////////////用于过滤部分掉部分分区，,在测试RATE性能约束时用到(测试无效)，当partitionMultiple为1时，不过滤
            val blockRegionEnd = block._2.indexOf('\n',block._2.indexOf('\n')+1)
            val blockReginString = block._2.substring(0,blockRegionEnd)
            val blockReginPeak = blockReginString.split("\n")
            val blockReginPeakArr = blockReginPeak.flatMap(peak=>peak.split(" ").map(str=>str.toDouble))
            val blockData = block._2.substring(blockRegionEnd+1,block._2.length)
            val blockRow =
//              if(blockData.isEmpty && filterFlag) //注意过滤标识
              if(blockData.isEmpty)
                new Array[Row](0)
              else
                blockData.split("\n").map{str=>str.split(" ")}.map{
                p => Row(
                  if(p(0)==null) null else p(0),
                  if(p(1)==null) null else p(1).toInt,
                  if(p(2)==null) null else p(2).toInt,
                  if(p(3)==null) null else p(3).toDouble,
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
            (block._1,blockReginPeakArr,blockRow)
        }
       //防止分片数过多，出现某些分片美哦与星数据情况
       blockMap=blockMap.filter(_._3.length != 0)
//       blockMap.coalesce(cpu,true)
       blockReginRDD = blockReginRDD + (spaceIdx->blockMap.cache())
        i+=1
    }
    (ccdReginRDD, blockReginRDD)
    }
  }

