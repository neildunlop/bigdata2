package com.datinko.prototype.bigdata2.core.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.joda.time.DateTime;
import org.junit.Test;

import java.time.DateTimeException;

/**
 * Created by Neil on 05/09/2015.
 */
public class DataGeneratorTests {

    @Test
    public void canGenerateRandomBetsForKnownEvents() throws JsonProcessingException {

        DataGenerator generator = new DataGenerator();

        generator.generateTotallyRandomBets(2000, 500, 10000);

    }

    @Test
    public void canGeneratePsuedoRandomAnonymousBetsForKnownEvents() throws JsonProcessingException {

        DataGenerator generator = new DataGenerator();

        DateTime startTime = DateTime.now().plusMinutes(16);
        generator.generatePseudoRandomAnonymousBets(startTime, 2000, 500, 10000);

    }


}
