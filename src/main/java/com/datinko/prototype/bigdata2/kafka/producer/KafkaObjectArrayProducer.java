package com.datinko.prototype.bigdata2.kafka.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Kafka Producer that reads csv bet objects from the file system and dispatches messages to a target Kafka topic as an object array.
 */
public class KafkaObjectArrayProducer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducer.class);
    private final Charset ENCODING = Charset.forName("UTF-8");

    private String topic = "";
    private InputStream inputStream = null;

    private ProducerConfig producerConfig = null;
    private Producer<Integer, String> producer = null;


    public KafkaObjectArrayProducer(String topic, InputStream stream)
    {
        this.topic = topic;
        this.inputStream = stream;
    }

    protected void configure() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");

        producerConfig = new ProducerConfig(props);
    }

    public static Object[] parseBetToObjectArray(String betDataToSend) {

        Object[] rawData = betDataToSend.split(",");
        Object[] dataToSend = new Object[rawData.length];

        for(int i=0; i<rawData.length; i++) {

            if(i==9) {
                dataToSend[i] = Float.parseFloat(rawData[i].toString());
            }
            else {
                dataToSend[i] = rawData[i];
            }
        }
        return dataToSend;
    }

    public void run() {

        configure();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);  //means a limit of 100 concurrent runnables
        BufferedReader reader = null;

        try {
            try {
                reader = new BufferedReader(
                        new InputStreamReader(this.inputStream, ENCODING));

                String rawLine;
                LOGGER.debug("Producing messages");
                producer = new Producer<Integer, String>(producerConfig);

                while ((rawLine = reader.readLine()) != null) {

                    final String line = rawLine;
                    //need to read the timestamp from the object array
                    final Object[] betDataArray = rawLine.split(",");

                    //this is what we actually run when the future becomes due...
                    Runnable task = () -> {
                        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                        //System.out.println("Firing bet " + betDataArray[0] + " at " + DateTime.now().toString(fmt));
                        System.out.println("Firing bet: " + DateTime.now().toString(fmt) + "[" + line + "]");

                        producer.send(new KeyedMessage<Integer, String>(
                                this.topic, "[" + line + "]"));
                    };

                    DateTime d1 = DateTime.now();
                    DateTime d2 = DateTime.parse(betDataArray[1].toString());

                    long diffInMillis = d2.getMillis() - d1.getMillis();

                    if(diffInMillis<0) {
                        diffInMillis = 0;
                    }

                    //Schedule the runnable task to run after a delay of three seconds
                    ScheduledFuture<?> future = executor.schedule(task, diffInMillis, TimeUnit.MILLISECONDS);
                }
                LOGGER.debug("Done scheduling messages");

                //wait for keypress before killing the producer
                System.in.read();

            } catch (IOException ex) {
                LOGGER.fatal("IO Error while producing messages", ex);
                LOGGER.trace(null, ex);
            }
        } catch (Exception ex) {
            LOGGER.fatal("Error while producing messages", ex);
            LOGGER.trace(null, ex);
        } finally {
            try {
                if (reader != null) reader.close();
                if (producer != null) producer.close();
            } catch (IOException ex) {
                LOGGER.fatal("IO error while cleaning up", ex);
                LOGGER.trace(null, ex);
            }
        }
    }
}
