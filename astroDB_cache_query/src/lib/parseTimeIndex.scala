package lib

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.redislabs.provider.redis._

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.immutable.HashSet
/**
  * Created by yc on 2017/6/6.
  */

//pixelAbStarId() (SEPI index) and pixelAbStarId_ICDE2013() (EPI index) need the time index
// for each abnormal star has two points (the left point and right point)
//pixelAbStarId_ICDE2013() which parses the time index completely is
// the approach which is according with the paper "Interval indexing and querying on key-value cloud stores"
class parseTimeIndex(sc:SparkContext, time:Array[Double],
                     blocks:Array[(String,RDD[(String, Array[Double],Array[Row])])],
                     redisSliceNum:Int) {

  def pixelAbStarId(): Array[RDD[String]] = {

    val leftVal = time(0)
    val rightVal = time(1)
    val blocksRDD = blocks.map{turple =>
    val a=turple._2.map{tmp=>tmp._1.split("_", 2)(1)}.collect()
      (turple._1.split("_")(1),a.toSet)
    }.toMap

    var starName = sc.parallelize(Seq[String](""))

//    val aaa=1

    val starNameSet = blocksRDD.map {
      ccdNo =>
        val abStarKeyRex = "idx_ab_ref_" + ccdNo._1 + "_*"
        if (leftVal == Double.NegativeInfinity && rightVal == Double.PositiveInfinity) {
          val abStarKeyFilter = sc.fromRedisZSet(abStarKeyRex, redisSliceNum)
            .filter{key =>
              var isExist = false
              val suffix = key.split("_", 3)(2)
              val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
              val blockNo = suffix.substring(0, endIdx)
              if(ccdNo._2.contains(blockNo))
              isExist=true
              isExist
          }

          starName = abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }

        } else if (leftVal == Double.NegativeInfinity || rightVal == Double.PositiveInfinity) {

          val abStarKeyFilter = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, rightVal, redisSliceNum)
            .filter{
              key =>
                var isExist = false
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                if(ccdNo._2.contains(blockNo))
                  isExist=true
                isExist
          }

          starName = abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }
        } else {
          val abStarKeyFilter
          = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, Double.PositiveInfinity, redisSliceNum)
            .filter{
              key =>
                var isExist = false
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                if(ccdNo._2.contains(blockNo)) {
                  val l = suffix.lastIndexOf("_")
                  val ll = suffix.lastIndexOf("_", l - 1) + 1
                  val timeBegin = suffix.substring(ll, l).toDouble
                  if (timeBegin > rightVal)
                    isExist = false
                  else
                    isExist = true
                }
                isExist
            }

          starName = abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }
        }

        starName
    }
    starNameSet.toArray
  }

  def pixelAbStarId_ICDE2013(): Array[RDD[String]] = {

    val leftVal = time(0)
    val rightVal = time(1)
    var starName = sc.parallelize(Seq[String](""))

    val blocksRDD = blocks.map{turple =>
      val a=turple._2.map{tmp=>tmp._1.split("_", 2)(1)}.collect()
      (turple._1.split("_")(1),a.toSet)
    }.toMap

    val starNameSet = blocksRDD.map {
      ccdNo =>
        val abStarKeyRex = "idx_ab_ref_" + ccdNo._1 + "_*"
        if (leftVal == Double.NegativeInfinity && rightVal == Double.PositiveInfinity) {
          val abStarKeyFilter = sc.fromRedisZSet(abStarKeyRex, redisSliceNum)
            .filter{
              key =>
                var isExist = false
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                if(ccdNo._2.contains(blockNo))
                  isExist=true
                isExist
            }

          starName = abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }.distinct()

        } else if (leftVal == Double.NegativeInfinity || rightVal == Double.PositiveInfinity) {

          val abStarKeyFilter = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, rightVal, redisSliceNum)
            .filter{
              key =>
                var isExist = false
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                if(ccdNo._2.contains(blockNo))
                  isExist=true
                isExist
            }


          starName = abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }.distinct()
        } else {

          val abStarKeyFilter
          = sc.fromRedisZRangeByScore(abStarKeyRex, rightVal, Double.PositiveInfinity, redisSliceNum)
            .filter{
              key =>
                var isExist = false
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                if(ccdNo._2.contains(blockNo)) {
                  if (key.last == 'r') {
                    val l = suffix.lastIndexOf("_")
                    val ll = suffix.lastIndexOf("_", l - 1) + 1
                    val timeBegin = suffix.substring(ll, l).toDouble
                    if (timeBegin <= leftVal)
                      isExist = true
                  }
                }
                isExist
            }
          //    val aaaaaa = abStarKey.collect()

          starName = abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }
          //    val aaaa = starName.collect()

          if (leftVal != rightVal) {
            val abStarKeyFilter
            = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, rightVal, redisSliceNum)
              .filter{
                key =>
                  var isExist = false
                  val suffix = key.split("_", 3)(2)
                  val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                  val blockNo = suffix.substring(0, endIdx)
                  if(ccdNo._2.contains(blockNo))
                    isExist=true
                  isExist
              }

            starName ++= abStarKeyFilter.map { a => a.substring(0, a.lastIndexOf("_")) }.distinct()
          }
        }
        starName = starName.repartition(redisSliceNum)
        starName
    }
    starNameSet.toArray
  }


  def pixelAbStarIdWithJoin(): Array[RDD[String]] = {

    val leftVal = time(0)
    val rightVal = time(1)
    val blocksRDD = blocks.map{turple =>
      val a=turple._2.map{tmp=>(tmp._1.split("_", 2)(1),1)}
      (turple._1.split("_")(1),a)
    }

    var starName = sc.parallelize(Seq[String](""))

    //    val aaa=1

    val starNameSet = blocksRDD.map {
      ccdNo =>
        val abStarKeyRex = "idx_ab_ref_" + ccdNo._1 + "_*"
        if (leftVal == Double.NegativeInfinity && rightVal == Double.PositiveInfinity) {
          val abStarKV = sc.fromRedisZSet(abStarKeyRex, redisSliceNum)
            .map{key =>
              val suffix = key.split("_", 3)(2)
              val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
              val blockNo = suffix.substring(0, endIdx)
              (blockNo,key)
            }

          val abStarKeyFilter = abStarKV.join(ccdNo._2)

          starName = abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }

        } else if (leftVal == Double.NegativeInfinity || rightVal == Double.PositiveInfinity) {

          val abStarKV = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, rightVal, redisSliceNum)
            .map{
              key =>
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                (blockNo,key)
            }
          val abStarKeyFilter = abStarKV.join(ccdNo._2)

          starName = abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }
        } else {
          val abStarKV = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, Double.PositiveInfinity, redisSliceNum)
            .map{
              key =>
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                (blockNo,key)
            }
          val abStarKeyFilter = abStarKV.join(ccdNo._2).filter {
            k =>
              var isExist = false
              val key = k._2._1
              val l = key.lastIndexOf("_")
              val ll = key.lastIndexOf("_", l - 1) + 1
              val timeBegin = key.substring(ll, l).toDouble
              if (timeBegin > rightVal)
                isExist = false
              else
                isExist = true
              isExist
          }

          starName = abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }
        }

        starName
    }
    starNameSet
  }

  def pixelAbStarIdWithJoin_ICDE2013(): Array[RDD[String]] = {

    val leftVal = time(0)
    val rightVal = time(1)
    var starName = sc.parallelize(Seq[String](""))

    val blocksRDD = blocks.map{turple =>
      val a=turple._2.map{tmp=>(tmp._1.split("_", 2)(1),1)}
      (turple._1.split("_")(1),a)
    }

    val starNameSet = blocksRDD.map {
      ccdNo =>
        val abStarKeyRex = "idx_ab_ref_" + ccdNo._1 + "_*"
        if (leftVal == Double.NegativeInfinity && rightVal == Double.PositiveInfinity) {
          val abStarKV = sc.fromRedisZSet(abStarKeyRex, redisSliceNum)
            .map{
              key =>
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                (blockNo,key)
            }
          val abStarKeyFilter = abStarKV.join(ccdNo._2)
          starName = abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }.distinct()

        } else if (leftVal == Double.NegativeInfinity || rightVal == Double.PositiveInfinity) {

          val abStarKV = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, rightVal, redisSliceNum)
            .map{
              key =>
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                (blockNo,key)
            }

          val abStarKeyFilter = abStarKV.join(ccdNo._2)
          starName = abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }.distinct()
        } else {

          val abStarKV = sc.fromRedisZRangeByScore(abStarKeyRex, rightVal, Double.PositiveInfinity, redisSliceNum)
            .map{
              key =>
                val suffix = key.split("_", 3)(2)
                val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                val blockNo = suffix.substring(0, endIdx)
                (blockNo,key)
            }
          //    val aaaaaa = abStarKey.collect()
          val abStarKeyFilter = abStarKV.join(ccdNo._2).filter{
            k =>
              val key = k._2._1
              var isExist = false
              if (key.last == 'r') {
                val l = key.lastIndexOf("_")
                val ll = key.lastIndexOf("_", l - 1) + 1
                val timeBegin = key.substring(ll, l).toDouble
                if (timeBegin <= leftVal)
                  isExist = true
              }
              isExist
          }

          starName = abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }
          //    val aaaa = starName.collect()

          if (leftVal != rightVal) {
            val abStarKV = sc.fromRedisZRangeByScore(abStarKeyRex, leftVal, rightVal, redisSliceNum)
              .map{
                key =>
                  val suffix = key.split("_", 3)(2)
                  val endIdx = suffix.indexOf("_", suffix.indexOf('_') + 1)
                  val blockNo = suffix.substring(0, endIdx)
                  (blockNo,key)
              }
            val abStarKeyFilter = abStarKV.join(ccdNo._2)

            starName ++= abStarKeyFilter.map { a => a._2._1.substring(0, a._2._1.lastIndexOf("_")) }.distinct()
          }
        }
        starName = starName.repartition(redisSliceNum)
        starName
    }
    starNameSet
  }
}