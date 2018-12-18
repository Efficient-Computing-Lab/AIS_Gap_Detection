case class Position(shipId: Int, status: Int, stationId: Int, speed: Double, lon: Double, lat: Double, cog: Double, heading: Double, timestamp: Int) extends Serializable  {

  override def toString: String = {
    s"$shipId,$status,$stationId,$speed,$lon,$lat,$cog,$heading,$timestamp"
  }

}
