package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo = pickupInfo.sort("x", "y", "z")
    pickupInfo.createOrReplaceTempView("pickupinfo")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    spark.udf.register("square", (inputX: Int) => HotcellUtils.square(inputX))

    spark.udf.register("getNeighboursNumber", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
    => HotcellUtils.getNeighboursNumber(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))

    spark.udf.register("getGetisOrdScore", (sumOfCount:Int,  numOfNeighbors:Int, mean: Double, std:Double, totalNumberOfCell: Int)
    => HotcellUtils.getGetisOrdScore(sumOfCount,  numOfNeighbors, mean, std, totalNumberOfCell))

    val givenPoints = spark.sql("select x, y, z from pickupinfo where x >= " + minX + " and y >= " + minY  + " and z >= " + minZ + " and x <= " + maxX + " and y <= " + maxY +  " and z <= " + maxZ ).persist()
    givenPoints.createOrReplaceTempView("givenPoints")
    givenPoints.show()

    // Get the points and the number of values for each set
    val pointsAndCount = spark.sql("select x, y, z, count(*) as pointValues from givenPoints group by x, y, z").persist()
    pointsAndCount.createOrReplaceTempView("pointsAndCount")
    pointsAndCount.show()

    // Calculate the sum and the sum of the Squares of the points.
    val sumofPoints = spark.sql("select sum(pointValues) as sumVal, sum(square(pointValues)) as squaredSum from pointsAndCount").persist()
    sumofPoints.createOrReplaceTempView("sumofPoints")
    sumofPoints.show()

    val sumVal = sumofPoints.first().getLong(0)
    val squaredSum = sumofPoints.first().getDouble(1)

    val mean = sumVal.toDouble / numCells.toDouble
    println(mean)
    val SD = math.sqrt((squaredSum.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))
    println(SD)



    val Neighbours = spark.sql(
      "select getNeighboursNumber( "+ minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x, a1.y, a1.z) as numberOfNeighbours," +
        "a1.x as x, a1.y as y, a1.z as z, " +   // x,y and z value in pointsAndCount
        //"a2.x as x2, a2.y as y2, a2.z as z2," +
        "sum(a2.pointValues) as sumOfCount " +  //this get all neighbours to a1.
        "from pointsAndCount as a1, pointsAndCount as a2 " +
        "where " +
            "(abs(a2.x - a1.x) < 2) " +      // this get all the neighbours of a1
            "and (abs(a2.y - a1.y) < 2 ) " +
            "and (abs(a2.z - a1.z) < 2 ) " +
                "group by a1.x, a1.y, a1.z order by a1.x, a1.y, a1.z").persist()
    Neighbours.createOrReplaceTempView("NeighboursCount")
    Neighbours.show()

    val gScore = spark.sql("select x,y,z, getGetisOrdScore(sumOfCount, numberOfNeighbours," + mean +","+ SD +","+ numCells +") as gScore from NeighboursCount Order by gScore desc")
    gScore.createOrReplaceTempView("gScore")
    gScore.show()

    val Result = spark.sql("select x,y,z from gScore").limit(50)
    Result.createOrReplaceGlobalTempView("Result")
    Result.show()

    return Result // YOU NEED TO CHANGE THIS PART
  }
}
