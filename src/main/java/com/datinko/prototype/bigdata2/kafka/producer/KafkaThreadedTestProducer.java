package com.datinko.prototype.bigdata2.kafka.producer;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * A Kafka producer that reads data from a file and sends it to a named Kafka topic.  Fire it up
 * and pass the name of the topic you want to send data to and the name of the file that contains
 * the source data.
 */
public class KafkaThreadedTestProducer {

    private static final Logger LOGGER = Logger.getLogger(KafkaThreadedTestProducer.class);
    private static final int BUFFER_LEN = 4096;

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("Not enough arguments were passed");
        }

        BasicConfigurator.configure();
        for (int i = 1; i < args.length; i++) {
            produceForFile(args[0], args[i]);
        }
    }

    private static void produceForFile(String topic, String filename) {
        try {
            LOGGER.debug("Setting up streams");
            PipedInputStream send = new PipedInputStream(BUFFER_LEN);
            PipedOutputStream input = new PipedOutputStream(send);

            LOGGER.debug("Setting up connections");
            LOGGER.debug("Setting up file reader");
            BufferedFileReader reader = new BufferedFileReader(filename, input);
            LOGGER.debug("Setting up kafka producer");
            KafkaProducer kafkaProducer = new KafkaProducer(topic, send);

            LOGGER.debug("Spinning up threads");
            Thread source = new Thread(reader);
            Thread kafka = new Thread(kafkaProducer);

            source.start();
            kafka.start();

            LOGGER.debug("Joining");
            kafka.join();
        } catch (IOException ex) {
            LOGGER.fatal("IO Error while piping", ex);
            LOGGER.trace(null, ex);
        } catch (InterruptedException ex) {
            LOGGER.warn("interruped", ex);
            LOGGER.trace(null, ex);
        }
    }
}
