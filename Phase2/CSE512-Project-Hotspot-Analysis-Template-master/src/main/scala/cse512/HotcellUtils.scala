package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def square (a:Int) : Double = {
    (a * a).toDouble
  }

  def getNeighboursNumber (x:Int, y:Int, z:Int, xMax:Int, yMax:Int,zMax:Int,xMin:Int, yMin:Int,zMin:Int): Int = {
    var numOfEdge :Int  = 0
    if( x == xMax || x == xMin){
      numOfEdge += 1
    }
    if(y == yMax || y == yMin) {
      numOfEdge += 1
    }
    if (z == zMax || z == zMin) {
      numOfEdge += 1
    }
    var returnVal : Int = 26
    numOfEdge match {
      case 1=> returnVal =  17
      case 2=> returnVal =  10
      case 3=> returnVal =   7
      case 0=> returnVal =  26
    }
    return returnVal
  }

  def getGetisOrdScore(sumOfCount:Int, numOfNeighbors:Int,mean: Double, std:Double, totalNumberOfCell: Int) : Double = {
    val numerator = sumOfCount.toDouble - (mean * numOfNeighbors.toDouble)
    val denominator = std * math.sqrt(( (totalNumberOfCell * numOfNeighbors.toDouble) - (numOfNeighbors.toDouble * numOfNeighbors.toDouble))/ (totalNumberOfCell-1.0))
    return numerator/denominator
  }

  // YOU NEED TO CHANGE THIS PART
}
