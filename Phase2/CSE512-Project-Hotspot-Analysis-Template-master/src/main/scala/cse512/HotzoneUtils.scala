package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
   val Rectangle_Points = queryRectangle.split(",")

   var max_x:Float = math.max(Rectangle_Points(2).toFloat,Rectangle_Points(0).toFloat)
   var min_x:Float = math.min(Rectangle_Points(2).toFloat,Rectangle_Points(0).toFloat)
   var max_y:Float = math.max(Rectangle_Points(3).toFloat,Rectangle_Points(1).toFloat)
   var min_y:Float = math.min(Rectangle_Points(3).toFloat,Rectangle_Points(1).toFloat)

   val Point = pointString.split(",")
   var x:Float = Point(0).toFloat
   var y:Float = Point(1).toFloat

   if ((x <= max_x && x >= min_x) && (y <= max_y && y >= min_y))
   {
     true
   }
   else {
     false
   }
  }

  // YOU NEED TO CHANGE THIS PART

}
