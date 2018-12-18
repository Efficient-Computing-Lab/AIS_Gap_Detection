import java.io.FileWriter
import java.text.SimpleDateFormat

object Utils {

  var LONGITUDE_GRID_START = -180.0
  var LATITUDE_GRID_START = -90.0
  // step to move on to the next horizontal grid cell
  val LONGITUDE_STEP = 0.2
  // step to move on to the next vertical grid cell
  val LATITUDE_STEP = 0.2
  // assumption: Lon/Lat have at most 6 decimal digits
  val MU = 1000000

  // timestamp -> (lon,lat -> stationId,numPositions)
  var grid: Map[Int,Map[(Double,Double),(Int,Int)]] = Map()

  /** Reads coverage grid from a file
    * Grid csv file contains columns -> LON,LAT,STATION,COUNTER,TIM
    * @param filename
    */
  def readAndFlattenGrid(filename: String): Unit = {
    scala.io.Source.fromFile(filename).getLines.foreach{
      line =>
        val parts = line.split(",")
        val lon = parts.head.trim.toDouble
        val lat = parts(1).toDouble
        val stationId = parts(2).toInt
        val numPositions = parts(3).toInt
        val dateAndTime = parts.last.split("[ ]").head + " 00:00:00"
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val timestamp = (sdf.parse(dateAndTime).getTime/1000).toInt

        grid.get(timestamp) match {
          case Some(cellMap) => {
            var updatedMap = cellMap
            updatedMap += ((lon,lat) -> (stationId,numPositions))
            grid += (timestamp -> updatedMap)
          }
          case None => grid += (timestamp -> Map((lon,lat) -> (stationId,numPositions)))
        }
    }
  }

  /**
    * Returns the network coverage of previous day
    * This method may be slow. Ideally, in a real-time system this method is useless,
    * because an updated map will be retrieved
    * @param timestamp position timestamp
    * @return
    */
  def getCoverageNetwork(timestamp: Int): Option[Map[(Double,Double),(Int,Int)]] = {
    // inequality returns all the previous days. Max function returns the previous day
    val network = grid.filterKeys(t => timestamp >= t)
    if (network.nonEmpty) {
      val networkCoverage = network.maxBy(_._1)._2
      Some(networkCoverage)
    }
    else {
      println("emptyyyyyyyyyyyyyyyy")
      None
    }
  }

  /**
    * Returns the enclosing cell based on the coordinates given
    * @param lon longitude
    * @param lat latitude
    * @return enclosing cell id
    */
  def getEnclosingGridCell(lon: Double, lat: Double): (Double,Double) = {
    val x = Math.round(lon * MU)
    val y = Math.round(lat * MU)
    val xStart = Math.round(LONGITUDE_GRID_START * MU)
    val xStep = Math.round(LONGITUDE_STEP * MU)
    val yStart = Math.round(LATITUDE_GRID_START * MU)
    val yStep = Math.round(LATITUDE_STEP * MU)
    val llx = x - ((x - xStart) % xStep)
    val lly = y - ((y - yStart) % yStep)
    val lowLeftX = llx.toDouble / MU
    val lowLeftY = lly.toDouble / MU
    (lowLeftX,lowLeftY)
  }

  /**
    * This uses the ‘haversine’ formula to calculate
    * the great-circle distance between two points – that is,
    * the shortest distance over the earth’s surface – giving an ‘as-the-crow-flies’ distance
    * between the points
    * (ignoring any hills they fly over, of course)
    * @param lon1 longitude 1
    * @param lat1 latitude 1
    * @param lon2 longitude 2
    * @param lat2 latitude 2
    * @return distance in kilometers
    */
  def getHarvesineDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val lon1Rad = lon1*(Math.PI/180)
    val lat1Rad = lat1*(Math.PI/180)
    val lon2Rad = lon2*(Math.PI/180)
    val lat2Rad = lat2*(Math.PI/180)
    val dLon = lon2Rad - lon1Rad
    val dLat = lat2Rad - lat1Rad
    val a = Math.pow(Math.sin(dLat/2),2) + Math.cos(lat1Rad)*Math.cos(lat2Rad)*Math.pow(Math.sin(dLon/2),2)
    val c = 2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a))
    // earth radius = 3961 miles / 6373 km
    6373*c
  }

  /**
    * Returns the coordinates of the vessel at future time stamp
    * based on current position, speed and heading
    * @param lon current longitude of vessel
    * @param lat current latitude of vessel
    * @param speed current speed of vessel in km/h
    * @param heading current heading of vessel
    * @param timeToProject future time stamp
    * @return
    */
  def getProjection(p: Position, timeToProject: Int): (Double,Double) = {
    val lonToRadians = p.lon.toRadians
    val latToRadians = p.lat.toRadians
    val headingToRadians = p.cog.toRadians

    // 6373 km is the earth`s radius
    val delta = timeToProject * (p.speed*1.852) / 3600 / 6373

    // calculate the future latitude
    val projectedLat: Double = BigDecimal(Math.asin((Math.sin(latToRadians) * Math.cos(delta)) + (Math.cos(latToRadians) * Math.sin(delta) *
      Math.cos(headingToRadians))).toDegrees).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
    // calculate the future longitude
    val projectedLon: Double = BigDecimal((lonToRadians + Math.atan2(Math.sin(headingToRadians) * Math.sin(delta) * Math.cos(latToRadians),
      Math.cos(delta) - (Math.sin(latToRadians) * Math.sin(projectedLat.toRadians)))).toDegrees).setScale(6,
      BigDecimal.RoundingMode.HALF_UP).toDouble

    (projectedLon,projectedLat)
  }

  /**
    * Returns the stream iterator mapped to Position case class
    * @param filename stream file
    * @return
    */
  def readStream(filename: String): Iterator[Position] = {
    scala.io.Source.fromFile(filename)
      .getLines
      .map{
        p =>
          // SHIP_ID,STATUS,STATION,SPEED,LON,LAT,COURSE,HEADING,TIMESTAMP
          val parts = p.split(",")
          val shipId = parts.head.toInt
          val status = parts(1).toInt
          val stationId = parts(2).toInt
          val speed = parts(3).toDouble/10
          val lon = parts(4).toDouble
          val lat = parts(5).toDouble
          val cog = parts(6).toDouble
          val heading = parts(7).toDouble
          val dateAndTime = parts.last

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          val timestamp = (sdf.parse(dateAndTime).getTime/1000).toInt
          Position(shipId,status,stationId,speed,lon,lat,cog,heading,timestamp)
      }
  }

  def getTwoVessels(filename: String, vessel1: String, vessel2: String): Unit = {
    val w = new FileWriter("test2.csv",true)
    scala.io.Source.fromFile(filename).getLines.foreach{
      line =>
        val id = line.split(",").head
        if (id == vessel1 || id == vessel2) w.write(line + "\n")
    }
    w.close
  }

}
