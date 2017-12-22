package lib

/**
  * Created by yc on 2017/6/2.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.immutable
class parseSpaceIndex(spaceIdx:(RDD[(String,Array[Double])],
  immutable.HashMap[String,RDD[(String, Array[Double],Array[Row])]])) {

def pixelRound(param:Array[String]):
Array[(String,RDD[(String, Array[Double],Array[Row])])] ={

  val x = param(0).toDouble
  val y = param(1).toDouble
  val r = param(2).toDouble
  val maxX = x + r
  val minX = x - r
  val maxY = y + r
  val minY = y - r

  val needCCD = spaceIdx._1.filter{
    partition =>
      val p_minX = partition._2(0)
      val p_minY = partition._2(1)
      val p_maxX = partition._2(2)
      val p_maxY = partition._2(3)
      var  returnFlag = false

      // avoid the r is very small, the search round is totally covered by a block
      // find the block which contains the center of the search round
      //搜索圆完全在某个块内
      if((p_minX <= minX && p_maxX >= maxX) && (p_minY <= minY && p_maxY >= maxY))
        returnFlag = true
      //the search round only is penetrated by horizontal strips
      //搜索圆仅被横条分割
      else if(p_minX <= minX && p_maxX >= maxX) { //localtion of x
        if ((p_minY <= maxY && p_minY >= minY) || (p_maxY <= maxY && p_maxY >= minY)) //localtion of y
          returnFlag = true
        ////the search round only is penetrated by vertical strips
        //搜索圆仅被竖条分割
      } else if(p_minY <= minY && p_maxY >= maxY) { //localtion of y
        if ((p_minX <= maxX && p_minX >= minX) || (p_maxX <= maxX && p_maxX >= minX))//localtion of x
          returnFlag = true
        ////the search round is penetrated by horizontal-vertical strips
        //搜索圆被块分割
        //下述条件，判断搜索圆相切矩形所包含的块
      } else if ((p_minX <= maxX && p_minX >= minX) || (p_maxX <= maxX && p_maxX >= minX)) {
        if((p_minY <= maxY && p_minY >= minY) || (p_maxY <= maxY && p_maxY >= minY)) {
          val need_X = if((p_minX - x).abs < (p_maxX - x).abs) p_minX else p_maxX
          val need_Y = if((p_minY - y).abs < (p_maxY - y).abs) p_minY else p_maxY
          //相切矩形所包含的块的最近端点在搜索圆内
          if(Math.hypot(need_X-x, need_Y-y)<=r)
            returnFlag = true
          //块端点不在圆内，但块贯穿圆
          else if((x<=p_maxX&&x>=p_minX) || (y<=p_maxY && y>=p_minX))
            returnFlag = true
        }
      }
      returnFlag
  }.map{_._1}.collect()



    //"spaceIdx_"+param(0)

//  //for local test
//  val simMap = spaceIdx.map{
//    m =>
//      val aa = m._2.collect()
//      m._1 -> aa
//  }

  //for local test
//  val needSpaceIdx = simMap(needCCD)



//val aaaa = 1

  val needPatition = needCCD.map{ ccd =>
    val p = spaceIdx._2(ccd).filter { partition =>
      val p_minX = partition._2(0)
      val p_minY = partition._2(1)
      val p_maxX = partition._2(2)
      val p_maxY = partition._2(3)
      var returnFlag = false

      // avoid the r is very small, the search round is totally covered by a block
      // find the block which contains the center of the search round
      //搜索圆完全在某个块内
      if ((p_minX <= minX && p_maxX >= maxX) && (p_minY <= minY && p_maxY >= maxY))
        returnFlag = true
      //the search round only is penetrated by horizontal strips
      //搜索圆仅被横条分割
      else if (p_minX <= minX && p_maxX >= maxX) {
        //localtion of x
        if ((p_minY <= maxY && p_minY >= minY) || (p_maxY <= maxY && p_maxY >= minY)) //localtion of y
          returnFlag = true
        ////the search round only is penetrated by vertical strips
        //搜索圆仅被竖条分割
      } else if (p_minY <= minY && p_maxY >= maxY) {
        //localtion of y
        if ((p_minX <= maxX && p_minX >= minX) || (p_maxX <= maxX && p_maxX >= minX)) //localtion of x
          returnFlag = true
        ////the search round is penetrated by horizontal-vertical strips
        //搜索圆被块分割
        //下述条件，判断搜索圆相切矩形所包含的块
      } else if ((p_minX <= maxX && p_minX >= minX) || (p_maxX <= maxX && p_maxX >= minX)) {
        if ((p_minY <= maxY && p_minY >= minY) || (p_maxY <= maxY && p_maxY >= minY)) {
          val need_X = if ((p_minX - x).abs < (p_maxX - x).abs) p_minX else p_maxX
          val need_Y = if ((p_minY - y).abs < (p_maxY - y).abs) p_minY else p_maxY
          //相切矩形所包含的块的最近端点在搜索圆内
          if (Math.hypot(need_X - x, need_Y - y) <= r)
            returnFlag = true
          //块端点不在圆内，但块贯穿圆
          else if ((x <= p_maxX && x >= p_minX) || (y <= p_maxY && y >= p_minX))
            returnFlag = true
        }
      }
      returnFlag
    }
    (ccd,p)
  }
//  val aaa = needPatition.map(_._2.collect())
  //  aaa.foreach(println)
  needPatition
  //for local test
//  spaceIdx(" ")
}


}
