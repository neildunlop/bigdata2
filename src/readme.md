Simple Kafka Producer
This is a simple threaded file based Kafka consumer. It reads data on one thread and buffers it over to the Kafka producer thread.
Execute with:
1. mvn clean package
2. java -jar .\target\bigdata2-1.0-SNAPSHOT-jar-with-dependencies.jar test c:\Users\Neil\IdealProjects\bigdata2\src\main\resources\flight.json

3. For the rest producer:  (I think!  untested!!)
java -jar .\target\bigdata2-1.0-SNAPSHOT-jar-with-dependencies.jar http://localhost/whateverendpoint c:\Users\Neil\IdealProjects\bigdata2\src\main\resources\highvolumebetdata

https://github.com/zdata-inc/SimpleKafkaProducer
http://zdatainc.com/2014/08/real-time-streaming-apache-spark-streaming/
http://zdatainc.com/2014/07/real-time-streaming-apache-storm-apache-kafka/

Setting up Kafka - 1. Download Apache Kafka from http://kafka.apache.org/downloads.html
                   2. Extract the zip contents to a local folder
                   3. Go to the Kafka installation directory/bin/windows 
                   4. Run Zookeeper by the following command
                   C:\kafka_2.11-0.8.2.1\bin\windows> zookeeper-server-start.bat ..\..\config\zookeeper.properties
                   5. Now run Kafka by using command prompt
                   C:\kafka_2.11-0.8.2.1\bin\windows>kafka-server-start.bat ..\..\config\server.properties
          

                   
Notes
=====
Check out executable JAR plugin that was used to make an executable JAR including dependencies:
http://javing.blogspot.co.uk/2014/06/creating-executable-jar-file-with-maven.html

Also, check out this kafka web console:
http://www.makedatauseful.com/web-console-for-kafka-messaging-system/
Getting the kafka console started use: c:\Users\Neil\Downloads\typesafe-activator-1.3.5-minimal\activator-1.3.5-minimal\activator "start -DapplyEvolutions.default=true"
(the positioning of the speech marks is important!
