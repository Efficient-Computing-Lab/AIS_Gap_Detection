import java.io.{File, PipedInputStream, PipedOutputStream}
import java.util.{Base64, Properties, UUID}

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import dk.dma.ais.message.{AisMessage5, AisPositionMessage}
import dk.dma.ais.packet.AisPacket
import dk.dma.ais.reader.AisReaders
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import java.util.function.Consumer
class Master extends Actor {

  val coordinator = context.actorSelection(s"akka.tcp://RemoteSystem@127.0.0.1:5150/user/coordinator")
  // variable that holds previous timestamp per ship id
  var previousTimestamps: Map[Int,Int] = Map()

  override def receive: Receive = {
    case "start" =>


      /*val encoded = new PipedOutputStream()
      val decoded = new PipedOutputStream()

      val ais_reader = AisReaders.createReaderFromInputStream(new PipedInputStream(encoded))
      ais_reader.registerPacketHandler((packet: AisPacket) => {
        try {
          val vdm = packet.getVdm
          if (vdm != null) {
            packet.getAisMessage match {
              case msg: AisPositionMessage =>
                val pos = msg.getValidPosition
                if (pos != null) {
                  val w = new java.io.FileWriter("stream.csv",true)
                  val p = Position(msg.getUserId,msg.getNavStatus,0,if (msg.getSog <= 1022) msg.getSog / 10.0 else 0.0,pos.getLongitude,pos.getLatitude,if (msg.getCog < 3600) msg.getCog / 10.0 else 0.0,if (0 <= msg.getTrueHeading && msg.getTrueHeading < 360) msg.getTrueHeading.toDouble else 0.0,vdm.getCommentBlock.getInt("c"))
                  w.write(p + "\n")
                  w.close
                  /*decoded.write(("P,"
                    + msg.getUserId + ","
                    + vdm.getCommentBlock.getLong("c") + ","
                    + pos.getLongitude + ","
                    + pos.getLatitude + ","
                    + (if (0 <= msg.getTrueHeading && msg.getTrueHeading < 360) msg.getTrueHeading else "") + ","
                    + (if (msg.getCog < 3600) msg.getCog / 10.0 else "") + ","
                    + (if (msg.getSog <= 1022) msg.getSog / 10.0 else "")
                    + "\n").getBytes)*/
                }
              case msg: AisMessage5 =>
                /*decoded.write(("S,"
                  + msg.getUserId + ","
                  + vdm.getCommentBlock.getLong("c") + ","
                  + msg.getShipType + ","
                  + Base64.getEncoder.encodeToString(msg.getDest.getBytes)
                  + "\n").getBytes)*/
            }
          }
        } catch {
          case _: Throwable =>
        }
      })
      ais_reader.start()

      val consumer_props = new Properties()
      //consumer_props.put("security.protocol", "plaintext")
      //consumer_props.put("sasl.mechanisms", "SCRAM-SHA-256")
      //consumer_props.put("sasl.username", "Value")
      //consumer_props.put("sasl.password", "Value")
      consumer_props.put("schema.registry.url", "http://kafka01.marinetraffic.com:8081")
      consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01.marinetraffic.com:9092")
      consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
      consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
      //consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, System.getProperty("system_name") + "-" + System.getProperty("node"))
      consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
      //consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val consumer = new KafkaConsumer[String, GenericData.Record](consumer_props)
      consumer.subscribe(Seq("mt.nmea.raw").asJava)

      var cnt = 0L
      var t = System.currentTimeMillis()
      val timeout = java.time.Duration.ofMillis(100)
      while (true) {
        for (rec <- consumer.poll(timeout).asScala) {
          val comment_block = "c:" + rec.value().get("timestamp").asInstanceOf[Int]

          encoded.write(("\\" + comment_block + "*" + comment_block.toCharArray.foldLeft(0)(_ ^ _).toHexString.toUpperCase() + "\\" + rec.value().get("nmeaMessage").asInstanceOf[org.apache.avro.util.Utf8].toString + "\n").getBytes)

          cnt += 1
          if (cnt % 100000 == 0) {
            val now = System.currentTimeMillis()

            println(cnt + ": " + (now - t))
            t = now
          }
        }
      }*/

      /*val consumer_props = new Properties()
      consumer_props.put("schema.registry.url", "http://kafka01.marinetraffic.com:8081")
      consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01.marinetraffic.com:9092")
      consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
      consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
      consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)

      val consumer = new KafkaConsumer[String, GenericData.Record](consumer_props)
      consumer.subscribe(Seq("mt.nmea.messages.position-report-class-a").asJava)

      val timeout = java.time.Duration.ofMillis(100)
      while (true) {
        for (rec <- consumer.poll(timeout).asScala) {
          //println(rec.value)
          val timestamp = rec.value().get("mt_timestamp").asInstanceOf[Int]
          val id = rec.value().get("mt_ship_id")
          if (rec.value().get("mt_ship_id") != null) {
            val shipId = id.asInstanceOf[Int]
            val navigational_status = rec.value().get("navigational_status")
            val mt_station_id = rec.value().get("mt_station_id")
            val sog = rec.value().get("sog")
            val lon = rec.value().get("lon")
            val lat = rec.value().get("lat")
            val cog = rec.value().get("cog")
            val true_heading = rec.value().get("true_heading")

            val navStatus = if (navigational_status != null) navigational_status.asInstanceOf[Int] else -1
            val stationId = if (mt_station_id != null) mt_station_id.asInstanceOf[Int] else -1
            val speed = if (sog != null) sog.asInstanceOf[Float] else -1.0
            val longitude = if (lon != null) lon.asInstanceOf[Float] else 0.0
            val latitude = if (lat != null) lat.asInstanceOf[Float] else 0.0
            val courseOverGround = if (cog != null) cog.asInstanceOf[Int].toDouble else -1.0
            val trueHeading = if (true_heading != null) true_heading.asInstanceOf[Int].toDouble else -1.0

            val position = Position(shipId,navStatus,stationId,speed,longitude,latitude,courseOverGround,trueHeading,timestamp)
            println(position)
          }


        }
      }*/


    //val stream = Utils.readStream("network_coverage_east_atlantic/test.csv")
      val stream = Utils.readStream("network_coverage_east_atlantic/raw_2018_10_09-15_east_atlantic.csv")

      while (stream.hasNext) {
        val msg = stream.next
        previousTimestamps.get(msg.shipId) match {
          case Some(previousTimestamp) =>
            if ((msg.timestamp - previousTimestamp) > 1200) {
              coordinator ! TimeOut(msg.shipId,1200) // did not send message for 20 minutes, send a time out
            }
          case None =>
        }
        coordinator ! msg
        previousTimestamps += (msg.shipId -> msg.timestamp)
        //Thread.sleep(500)
      }
      coordinator ! "END"
      println("Stream ended...")
  }
}

object App {

  def main(arg: Array[String]): Unit = {
    runSystem

    //Utils.getTwoVessels("network_coverage_east_atlantic/raw_2018_10_09-15_east_atlantic.csv","1647159","215716")

    //println(Utils.getEnclosingGridCell(-7.5,-33.7))

    /*Utils.readAndFlattenGrid("network_coverage_east_atlantic/network_coverage_east_atlantic.csv")
    val c = Utils.getCoverageNetwork(1539087555)
    println(c)*/


  }

  def runSystem: Unit = {

    Utils.readAndFlattenGrid("network_coverage_east_atlantic/network_coverage_east_atlantic.csv")

    val configFile = getClass.getClassLoader.getResource("local_application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    val system = ActorSystem("ClientSystem",config)
    val master = system.actorOf(Props[Master], name="master")

    master.tell("start",master)

  }

  def runApp: Unit = {

    // ship id, last sampling interval
    var lastInterval = 240
    var predictedCell = (0.0,0.0)

    Utils.readAndFlattenGrid("network_coverage_east_atlantic/network_coverage_east_atlantic.csv")

    Utils.readStream("network_coverage_east_atlantic/test.csv").foreach{
      position =>
        val coverageNetwork = Utils.getCoverageNetwork(position.timestamp)

        println("Current position: " + position)
        val currentCell = Utils.getEnclosingGridCell(position.lon,position.lat)
        println("Current cell: " + currentCell)
        val newPosition = Utils.getProjection(position,lastInterval)
        println("New position: " + newPosition)
        predictedCell = Utils.getEnclosingGridCell(newPosition._1,newPosition._2)
        println("Predicted cell: " + predictedCell)
        println("===================")




    }
  }

}
