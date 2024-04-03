## AIS gap detection


<p align="center">
  <img width="846" height="391" src="https://github.com/f-coda/ais_gap_detection/blob/master/grid.png" alt="Sublime's custom image"/>
</p>


### Abstract

Today, most of the maritime surveillance systems rely on the automatic identification system (AIS), which is compulsory for vessels of specific categories to carry. Anomaly detection typically refers to the problem of finding patterns in data that do not conform to expected behaviour. AIS switch-off is such a pattern that refers to the fact that many vessels turn off their AIS transponder in order to hide their whereabouts when travelling in waters with frequent piracy attacks or potential illegal activity, thus deceiving either the authorities or other piracy vessels. Furthermore, fishing vessels switch off their AIS transponders so as other fishing vessels do not fish in the same area. To the best of our knowledge limited work has focused on AIS switch-off in real-time. We present a system that detects such cases in real-time and can handle high velocity, large volume of streams of AIS messages received from terrestrial base stations. We evaluate the proposed system in a real-world dataset collected from AIS receivers and show the achieved detection accuracy.
---

##### Dependencies

- akka 2.5.1
- kafka 2.0.1
- kafka-avro-serializer 5.0.1
- dk.dma.ais.lib 2.2

### References

I. Kontopoulos, K. Chatzikokolakis, D. Zissis, K. Tserpes, G. Spiliopoulos "Real-time maritime anomaly detection: detecting intentional AIS switch-off", *International Journal of Big Data Intelligence (IJBDI), Vol. 7, No. 2*, 2020.  
