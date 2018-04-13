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

  // YOU NEED TO CHANGE THIS PART
  def weightedNeighbors(x: Int, y: Int, z: Int, cube: Array[Array[Array[Double]]]): Double = {
    
    var sum : Double = 0
    var x_len : Int = cube.length
    var y_len : Int = cube(0).length
    var z_len : Int = cube(0)(0).length

    for (i <- -1 until 2) {
      for (j <- -1 until 2) {
        for (k <- -1 until 2) {
          var x_neighbor : Int = x + i
          var y_neighbor : Int = y + j
          var z_neighbor : Int = z + k

          if (x_neighbor >= 0 && x_neighbor < x_len && y_neighbor >= 0 && y_neighbor < y_len && z_neighbor >= 0 && z_neighbor < z_len ) {
            sum +=  cube(x_neighbor)(y_neighbor)(z_neighbor)
          }
        } 
      }
    }
    return sum
  }

  def numberNeighbors(x: Int, y: Int, z: Int, cube: Array[Array[Array[Double]]]) : Int = {

    var side_counter : Int  = 0

    var x_len : Int = cube.length
    var y_len : Int = cube(0).length
    var z_len : Int = cube(0)(0).length

    if (x == 0 || x == x_len - 1) {
      side_counter += 1
    }
    if (y == 0 || y == y_len - 1) {
      side_counter += 1
    }
    if (z == 0 || z == z_len -1) {
      side_counter += 1
    }

    if (side_counter == 1) { // On Face
      return 18
    }
    else if (side_counter == 2) { // On Edge
      return 12
    }
    else if (side_counter == 3) { // On Corner
      return 8
    }
    else { // Middle
      return 27
    }
  }
}
