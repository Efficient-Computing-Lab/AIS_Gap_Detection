import java.io.FileWriter

import akka.actor.{Actor, ActorRef}

class Worker extends Actor {

  // empty position
  var lastKnownPosition = Position(0,0,0,0.0,0.0,0.0,0.0,0.0,0)

  override def preStart(): Unit = {
    //println(s"${self.toString} has been created...")
  }

  override def receive: Receive = {
    case msg: Position =>
      lastKnownPosition = msg
    case "END" =>
      sender ! "STREAM_END"
    case to: TimeOut =>
      Utils.getCoverageNetwork(lastKnownPosition.timestamp) match {
        case Some(network) =>
          val (x,y) = Utils.getProjection(lastKnownPosition,to.delay)
          val predictedCell = Utils.getEnclosingGridCell(x,y)

          if (network.contains(predictedCell)) {
            /*val w = new FileWriter(s"gaps/gaps${self.hashCode}.csv",true)
            w.write(lastKnownPosition.toString + "," + (to.delay+lastKnownPosition.timestamp) + ",has_coverage\n")
            w.close*/
            //writeAnomaly(true,lastKnownPosition,to.delay,self.hashCode)
            sender ! lastKnownPosition.toString + "," + (to.delay+lastKnownPosition.timestamp) + ",has_coverage\n"
          }
          else {
            /*val w = new FileWriter(s"gaps/gaps${self.hashCode}.csv",true)
            w.write(lastKnownPosition.toString + "," + (to.delay+lastKnownPosition.timestamp) + ",no_coverage\n")
            w.close*/
            //writeAnomaly(false,lastKnownPosition,to.delay,self.hashCode)
            sender ! lastKnownPosition.toString + "," + (to.delay+lastKnownPosition.timestamp) + ",no_coverage\n"
          }
        case None =>
          println("No network for previous day...")
      }
  }

  def writeAnomaly(hasCoverage: Boolean, msg: Position, delay: Int, actorId: Int): Unit = {
    val w = new FileWriter(s"gaps/gaps$actorId.csv", true)
    w.write(msg.toString + "," + (delay + msg.timestamp) + s",$hasCoverage\n")
    w.close
  }

}
