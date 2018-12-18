import java.io.{File, FileWriter}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

class Coordinator extends Actor {

  // empty position
  var lastKnownPosition = Position(0,0,0,0.0,0.0,0.0,0.0,0.0,0)
  // variable that holds the ship ids seen so far
  var idsSoFar: Set[Int] = Set()
  var anomalies = new mutable.Queue[String]

  var availableActors: Map[Int,ActorRef] = Map()

  val outputStream = context.actorOf(Props[Worker], name=s"output")

  override def receive: Receive = {
    case msg: Position =>
      if (idsSoFar.contains(msg.shipId)) {
        val worker = availableActors(msg.shipId)
        worker ! msg
      }
      else {
        val worker = context.actorOf(Props[Worker], name=s"worker${msg.shipId}") // create new worker when a new ship id is received
        idsSoFar += msg.shipId // add id to the list of ids
        availableActors += (msg.shipId -> worker)
        worker ! outputStream
        worker ! msg
      }
    case to: TimeOut =>
      val worker = availableActors(to.shipId)
      worker ! to
    case "END" =>
      availableActors.foreach(a => a._2 ! "END")
    case "STREAM_END" =>
      println("Stream ended.")
      val w = new FileWriter("gaps.csv",true)
      while (anomalies.nonEmpty) {
        w.write(anomalies.dequeue)
      }
      w.close
    case anomaly: String =>
      anomalies += anomaly
      if (anomalies.size == 10000) {
        val w = new FileWriter("gaps.csv",true)
        while (anomalies.nonEmpty) {
          w.write(anomalies.dequeue)
        }
        w.close
      }
  }
}

object Coordinator {
  def main(args: Array[String]): Unit = {

    Utils.readAndFlattenGrid("network_coverage_east_atlantic/network_coverage_east_atlantic.csv")

    //get the configuration file from classpath
    val configFile = getClass.getClassLoader.getResource("remote_application.conf").getFile
    //parse the config
    val config = ConfigFactory.parseFile(new File(configFile))
    //create an actor system with that config
    val system = ActorSystem("RemoteSystem" , config)
    //create a remote actor from actorSystem
    val coordinator = system.actorOf(Props[Coordinator], name="coordinator")
    println("Coordinator is alive")
  }
}
