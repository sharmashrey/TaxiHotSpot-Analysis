package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer

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
  pickupInfo.show()

  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("cells")
  var x_len : Int = (maxX - minX + 1).toInt
  var y_len : Int = (maxY - minY + 1).toInt
  var z_len : Int = (maxZ - minZ + 1).toInt
  var geospatial3d = Array.ofDim[Double](x_len, y_len, z_len)
  
  var cellAttrDf = spark.sql("select cells.x, cells.y, cells.z, COUNT(*) as attr from cells group by cells.x, cells.y, cells.z")
  var cell_res = cellAttrDf.coalesce(1)
  cell_res.createOrReplaceTempView("cell_attribute_weights")

  // The summation x is probably the sum of all the attribute weights
  var sum_x : Double = 0
  var sum_x_sq: Double = 0
  var attr_row = cell_res.select("*").collect()
  attr_row.foreach(t => {
    var x_cell : Int = t.getInt(0) // Issue here, x is negative
    var y_cell : Int = t.getInt(1)
    var z_cell : Int = t.getInt(2)

    var norm_x : Int = x_cell - minX.toInt
    var norm_y : Int = y_cell - minY.toInt
    var norm_z : Int = z_cell - minZ.toInt

    // Only consider points in range
    if (norm_x >= 0 && norm_x < x_len && norm_y >= 0 && norm_y < y_len && norm_z >= 0 && norm_z < z_len) {
      var attr_cell : Double = t.getLong(3).toDouble
      sum_x += attr_cell
      sum_x_sq += math.pow(attr_cell, 2.0)
    }
  }) 

  // XBar
  var x_bar = sum_x/numCells.toDouble

  // Standard Deviation
  var sd_op1 : Double = sum_x_sq/numCells
  var sd_op2 : Double = math.pow(x_bar, 2.0)
  var standard_dev : Double = math.sqrt(sd_op1 - sd_op2)

  // Populate the Spatial 3D Cube
  attr_row.foreach(t => {
    var x_cell : Int = t.getInt(0) 
    var y_cell : Int = t.getInt(1)
    var z_cell : Int = t.getInt(2)

    var norm_x : Int = x_cell - minX.toInt
    var norm_y : Int = y_cell - minY.toInt
    var norm_z : Int = z_cell - minZ.toInt

    if (norm_x >= 0 && norm_x < x_len && norm_y >= 0 && norm_y < y_len && norm_z >= 0 && norm_z < z_len) {
      var attr_cell : Double = t.getLong(3).toDouble
      geospatial3d(norm_x)(norm_y)(norm_z) += attr_cell
    }
  })

  var tree = TreeMap.empty[Double, List[Double]]
  for (i <- 0 until x_len) {
    for (j <- 0 until y_len) {
      for (k <- 0 until z_len) {
        var orig_x : Int = i + minX.toInt
        var orig_y : Int = j + minY.toInt
        var orig_z : Int = k + minZ.toInt

        var sum_neighors : Double = HotcellUtils.weightedNeighbors(i, j, k, geospatial3d)
        var num_neighbors : Int = HotcellUtils.numberNeighbors(i, j, k, geospatial3d)

        var numerator: Double = sum_neighors - (x_bar * num_neighbors)
        
        var denom_op1 : Double = numCells * num_neighbors
        var denom_op2 : Double = math.pow(num_neighbors.toDouble, 2.0)
        var denom_op3 : Double = (denom_op1 - denom_op2)/(numCells - 1)
        var denominator : Double = standard_dev * math.sqrt(denom_op3)

        var g_score : Double = (numerator/denominator)

        tree += (g_score -> List(orig_x.toDouble, orig_y.toDouble, orig_z.toDouble))
      }
    }
  }

  var values = new ListBuffer[List[Double]]()
  for ((k, v) <- tree) {
    var add = List(k, v(0), v(1), v(2))
    values += add
  }
  var val_list = values.toList.map(x => (x(0), x(1), x(2), x(3))) 

  var newCoordinateN = Seq("gscore", "x", "y", "z")

  import spark.implicits._
  var resDf = val_list.toDF(newCoordinateN:_*)
  var resC = resDf.coalesce(1)

  resC.createOrReplaceTempView("res")

  var sorted = spark.sql("SELECT CAST(x AS INT) x_i, CAST(y AS INT) y_i, CAST(z AS INT) z_i FROM res ORDER BY gscore DESC")
  //var sorted = resC.withColumn("x", col(resC("x").cast("INT")))
  sorted.show()
  return sorted // YOU NEED TO CHANGE THIS PART
}
}
