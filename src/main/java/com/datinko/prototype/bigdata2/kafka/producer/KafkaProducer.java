package com.datinko.prototype.bigdata2.kafka.producer;

import com.datinko.prototype.bigdata2.core.Bet;
import com.datinko.prototype.bigdata2.core.serializer.MoneyDeserializer;
import com.datinko.prototype.bigdata2.core.serializer.MoneySerializer;
import com.datinko.prototype.bigdata2.rest.producer.RestProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.joda.money.Money;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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

/**
 * Kafka Producer that reads full json bet objects from the file system and dispatches messages to a target Kafka topic.
 */
public class KafkaProducer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducer.class);
    private final Charset ENCODING = Charset.forName("UTF-8");

    private String topic = "";
    private InputStream inputStream = null;

    private ProducerConfig producerConfig = null;
    private Producer<Integer, String> producer = null;


    public KafkaProducer(String topic, InputStream stream)
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

    public void run() {

        configure();

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Money.class, new MoneyDeserializer());
        module.addSerializer(Money.class, new MoneySerializer());
        mapper.registerModule(module);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);  //means a limit of 100 concurrent runnables

        BufferedReader reader = null;

        try {
            try {
                reader = new BufferedReader(
                        new InputStreamReader(this.inputStream, ENCODING));

                String line;
                LOGGER.debug("Producing messages");
                producer = new Producer<Integer, String>(producerConfig);

                while ((line = reader.readLine()) != null) {

                    final String json = line;
                    Bet bet = mapper.readValue(line, Bet.class);

                    //this is what we actually run when the future becomes due...
                    Runnable task = () -> {
                        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                        System.out.println("Firing bet " + bet.getId() + " at "+ DateTime.now().toString(fmt));

                        producer.send(new KeyedMessage<Integer, String>(
                                this.topic, json));
                    };

                    DateTime d1 = DateTime.now();
                    DateTime d2 = bet.getTimestamp();

                    long diffInMillis = d2.getMillis() - d1.getMillis();

                    if(diffInMillis<0) {
                        diffInMillis = 0;
                    }

                    //Schedule the runnable task to run after a delay of three seconds
                    ScheduledFuture<?> future = executor.schedule(task, diffInMillis, TimeUnit.MILLISECONDS);
                }
                LOGGER.debug("Done sending messages");

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
