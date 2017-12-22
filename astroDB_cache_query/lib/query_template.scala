package lib

import javax.ws.rs.QueryParam


class query_template {



  def getQuery(queryName:String, queryParam: String): String =
  {
    var raTemp = ""
    var decTemp = ""
    var searchRadius = ""
    var starCluster = ""
    var timeMin = ""
    var timeMax = ""
    var magMin = ""
    var magMax = ""
    var timestamp = ""
    var ccd = ""
    var starID = ""
    var starIDSet = ""
    val paramSet = queryParam.split(" ")
     queryName match {
       case "qt0" =>
       case "qt1" =>
         magMin = paramSet(0)
         magMax = paramSet(1)
       case "qt2" =>
         raTemp = paramSet(0)
         decTemp = paramSet(1)
         searchRadius = paramSet(2)
       case "qt3" =>
         starID = paramSet(0)
       case "qt4" =>
        ccd = paramSet(0)
       case "qo0" =>
         starCluster = paramSet(0)
         starIDSet = paramSet(1)
         timeMin = paramSet(2)
         timeMax = paramSet(3)
       case "qo1" =>
         starCluster = paramSet(0)
         starIDSet = paramSet(1)
         timeMin = paramSet(2)
         timeMax = paramSet(3)
       case "qo2" =>
         starCluster = paramSet(0)
         starIDSet = paramSet(1)
         timeMin = paramSet(2)
         timeMax = paramSet(3)
       case _ =>
         sys.error(s"do not support query $queryName")
     }
    val  queryOriginal = Map( //star_id,mag,timestamp
      ("qo0",
        s"""SELECT * FROM $starCluster """.stripMargin),
      ("qo1",
        s"""SELECT star_id,mag,timestamp
            |FROM $starCluster
            |WHERE star_id in ($starIDSet) and timestamp >$timeMin and timestamp<$timeMax
            |ORDER BY star_id,timestamp
     """.stripMargin), //,count(timestamp) GROUP BY star_id,ccd_num
      ("qo2",
        s"""SELECT star_id,ccd_num,count(timestamp)
            |FROM $starCluster
            |WHERE star_id in ($starIDSet) and timestamp >$timeMin and timestamp<$timeMax
            |GROUP BY star_id,ccd_num
     """.stripMargin)
    )

    val queryTemp = Map(
      ("qt0",
        s"""SELECT *
           |FROM template
   """.stripMargin),
      ("qt1",
        s"""SELECT star_id
            |FROM template
            |WHERE mag >$magMin AND mag<$magMax
   """.stripMargin),
      ("qt2",
        s"""SELECT star_id
            |FROM template
            |WHERE 180/3.1415926*3600*acos(sin(radians(dec))*sin(radians($decTemp))+cos(radians(dec))*cos(radians($decTemp))*cos(radians(ra)- radians($raTemp))) <$searchRadius
       """.stripMargin),
      ("qt3",
        s"""SELECT ra,dec
            |FROM template
            |WHERE star_id = '$starID'
     """.stripMargin),
      ("qt4",
        s"""SELECT star_id
            |FROM template
            |WHERE ccd_num = $ccd
       """.stripMargin
        )
    )
  var querySen = ""
    queryName match {
      case "qt0" =>
        querySen=queryTemp(queryName)
      case "qt1" =>
        querySen=queryTemp(queryName)
      case "qt2" =>
        querySen=queryTemp(queryName)
      case "qt3" =>
        querySen=queryTemp(queryName)
      case "qt4" =>
        querySen=queryTemp(queryName)
      case "qo0" =>
        querySen=queryOriginal(queryName)
      case "qo1" =>
        querySen=queryOriginal(queryName)
      case "qo2" =>
        querySen=queryOriginal(queryName)

    }
    querySen
  }

}
