package com.datinko.prototype.bigdata2.core.rest.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import com.datinko.prototype.bigdata2.core.Bet;
import com.datinko.prototype.bigdata2.core.factories.random.RandomBetFactory;
import com.datinko.prototype.bigdata2.rest.producer.RestProducer;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by neild on 09/09/2015.
 */
public class RestProducerTest {

    @Ignore //needs the HTTP endpoint running.
    public void canPostBetToHttpEndpoint() {

        InputStream stream = new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        };
        RandomBetFactory factory = new RandomBetFactory();
        RestProducer restProducer = new RestProducer("http://localhost/betEndpoint", stream);
        Bet bet = factory.generateRandomBet();

        Object[] result = restProducer.postBetToHttpEndpoint(bet.toString());

        assertNotNull(result);
    }

    @Test
    public void canParseBetStringToObjectArray() {

        RandomBetFactory factory = new RandomBetFactory();
        Bet bet = factory.generateRandomBet();

        Object[] parsedBetArray = RestProducer.parseBetToObjectArray(bet.toString());

        assertNotNull(parsedBetArray);
    }
}
