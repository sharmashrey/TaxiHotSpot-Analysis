package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val rect_vals : Array[String] = queryRectangle.split(",")
      val x0 : Double = rect_vals(0).toDouble
      val y0 : Double = rect_vals(1).toDouble
      val x1 : Double = rect_vals(2).toDouble
      val y1 : Double = rect_vals(3).toDouble

      val max_x : Double = math.max(x0, x1)
      val min_x : Double = math.min(x0, x1)
      val max_y : Double = math.max(y0, y1)
      val min_y : Double = math.min(y0, y1)

      // top-left is (min_x, max_y)
      // bot-right is (max_x, min_y)
      val query_point : Array[String] = pointString.split(",")
      val q_x : Double = query_point(0).toDouble
      val q_y : Double = query_point(1).toDouble
      if(min_x <= q_x && q_x <= max_x && min_y <= q_y && q_y <= max_y) {
        return true
      }
      else {
        return false
      }
  }

  // YOU NEED TO CHANGE THIS PART

}
