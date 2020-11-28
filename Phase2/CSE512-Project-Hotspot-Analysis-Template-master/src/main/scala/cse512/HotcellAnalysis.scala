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

    // YOU NEED TO CHANGE THIS PART
    spark.udf.register("calculateSquare", (inputX: Int) => HotcellUtils.calculateSquare(inputX))

    spark.udf.register("getNeighbourValue", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
    => HotcellUtils.getNeighbourValue(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))

    spark.udf.register("calculateOrdScore", (countSum:Int,  totalNeighbors:Int, mean: Double, std:Double, totalCells: Int)
    => HotcellUtils.calculateOrdScore(countSum,  totalNeighbors, mean, std, totalCells))

    val points = spark.sql("select x, y, z from pickupinfo where x >= " + minX + " and y >= " + minY  + " and z >= " + minZ + " and x <= " + maxX + " and y <= " + maxY +  " and z <= " + maxZ ).persist()
    points.createOrReplaceTempView("points")
    points.show()

    val pointsAndCount = spark.sql("select x, y, z, count(*) as pointValues from points group by x, y, z").persist()
    pointsAndCount.createOrReplaceTempView("pointsAndCount")
    pointsAndCount.show()

    val pointsSum = spark.sql("select sum(pointValues) as sumValue, sum(calculateSquare(pointValues)) as sumOfSquare from pointsAndCount").persist()
    pointsSum.createOrReplaceTempView("pointsSum")
    pointsSum.show()

    val sumValue = pointsSum.first().getLong(0)
    val sumOfSquare = pointsSum.first().getDouble(1)

    val mean = sumValue.toDouble / numCells.toDouble
    val std = math.sqrt((sumOfSquare.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))



    val neighbors = spark.sql(
      "select getNeighbourValue( "+ minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x, a1.y, a1.z) as numberOfneighbors," +
        "a1.x as x, a1.y as y, a1.z as z, sum(a2.pointValues) as countSum " +
        "from pointsAndCount as a1, pointsAndCount as a2 " +
        "where " +
            "(abs(a2.x - a1.x) < 2) " +
            "and (abs(a2.y - a1.y) < 2 ) " +
            "and (abs(a2.z - a1.z) < 2 ) " +
                "group by a1.x, a1.y, a1.z order by a1.x, a1.y, a1.z").persist()
    neighbors.createOrReplaceTempView("neighborsCount")
    neighbors.show()

    val ordScore = spark.sql("select x,y,z, calculateOrdScore(countSum, numberOfneighbors," + mean +","+ std +","+ numCells +") as ordScore from neighborsCount Order by ordScore desc")
    ordScore.createOrReplaceTempView("ordScore")
    ordScore.show()

    val finalResult = spark.sql("select x,y,z from ordScore").limit(50)
    finalResult.createOrReplaceGlobalTempView("finalResult")
    finalResult.show()

    return finalResult // YOU NEED TO CHANGE THIS PART
  }
}
