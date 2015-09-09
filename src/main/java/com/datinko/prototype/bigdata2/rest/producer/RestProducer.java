package com.datinko.prototype.bigdata2.rest.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.web.client.RestTemplate;

/**
 * Rest Producer that reads files from the file system and posts messages to a target HTTP endpoint.
 */
public class RestProducer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RestProducer.class);
    private final Charset ENCODING = Charset.forName("UTF-8");

    private String targetUrl = "";
    private InputStream inputStream = null;

    public Object[] postBetToHttpEndpoint(String rawBetData) {

        Object[] dataToSend = parseBetToObjectArray(rawBetData);
        final String uri = targetUrl;

        RestTemplate restTemplate = new RestTemplate();
        Object[] result = restTemplate.postForObject(uri, dataToSend, Object[].class);

        System.out.println(result);
        return result;
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

    public RestProducer(String targetUrl, InputStream stream) {
        this.targetUrl = targetUrl;
        this.inputStream = stream;
    }

    public void run() {

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);  //means a limit of 100 concurrent runnables

        BufferedReader reader = null;

        try {
            try {
                reader = new BufferedReader(
                        new InputStreamReader(this.inputStream, ENCODING));

                String rawLine;
                LOGGER.debug("Producing messages");

                while ((rawLine = reader.readLine()) != null) {

                    final String line = rawLine;
                    //need to read the timestamp from the object array
                    final Object[] betDataArray = rawLine.split(",");

                    //this is what we actually run when the future becomes due...
                    Runnable task = () -> {
                        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                        System.out.println("Firing bet " + betDataArray[0] + " at " + DateTime.now().toString(fmt));

                        postBetToHttpEndpoint(line);

                    };

                    DateTime d1 = DateTime.now();
                    DateTime d2 = DateTime.parse(betDataArray[0].toString());

                    long diffInMillis = d2.getMillis() - d1.getMillis();

                    if (diffInMillis < 0) {
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
            } catch (IOException ex) {
                LOGGER.fatal("IO error while cleaning up", ex);
                LOGGER.trace(null, ex);
            }
        }
    }

}
