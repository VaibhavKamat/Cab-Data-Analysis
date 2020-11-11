package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    val ST_Contains = (queryRectangle:String, pointString:String) =>
    {
      val Rectangle_Points = queryRectangle.split(",")
      var min_x:Float = Rectangle_Points(0).toFloat
      var min_y:Float = Rectangle_Points(1).toFloat
      var max_x:Float = Rectangle_Points(2).toFloat
      var max_y:Float = Rectangle_Points(3).toFloat

      // If incase the min value is found to be lesser than the max.
      if (min_x > max_x)
      {
        var temp = min_x
        min_x = max_x
        max_x = temp
      }
      if (min_y > max_y) {
        var temp = min_y
        min_y = max_y
        max_y = temp
      }

      val Point = pointString.split(",")
      var x:Float = Point(0).toFloat
      var y:Float = Point(1).toFloat

      if ((x <= max_x && x >= min_x) && (y <= max_y && yp >= min_y))
      {true}
      else {false}
    }
    // END OF ST_Contains Function
    
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
  
  def ST_Within(point1:String, point2:String, distance:Double) : Boolean ={
    val xy_1 = point1.split(",")
    val x_1 = xy_1(0).trim().toDouble
    val y_1 = xy_1(1).trim().toDouble

    val xy_2 = point2.split(",")
    val x_2 = xy_2(0).trim().toDouble
    val y_2 = xy_2(1).trim().toDouble


    val ed = scala.math.pow(scala.math.pow((x_1 - x_2),2) + scala.math.pow((y_1 - y_2),2),0.5) 

    if(ed <= distance){
        return true
    }
    else{
        return false
    }

  }
}
