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

    //Use this test to generate the background betting stream IN JSON  (for use in Kafka Producer)
    @Test
    public void canGeneratePsuedoRandomAnonymousBetsForKnownEventsInJSON() throws JsonProcessingException {

        DataGenerator generator = new DataGenerator();

        DateTime startTime = DateTime.now().plusMinutes(16);
        generator.generatePseudoRandomAnonymousBetsInJSON(startTime, 25000, 500, 10000);
    }

    //Use this test to generate the background betting stream IN CSV  (for use in Zeppelin)
    @Test
    public void canGeneratePsuedoRandomAnonymousBetsForKnownEventsInCSV() throws JsonProcessingException {

        DataGenerator generator = new DataGenerator();

        DateTime startTime = DateTime.now().plusMinutes(16);
        generator.generatePseudoRandomAnonymousBetsInCSV(startTime, 25000, 500, 10000);
    }

    //Use this test to generate the high volume, high speed betting on one selection
    //Generates 100 bets on Middlesbrough To win within a maximum of 50 seconds
    @Test
    public void canGenerateHighVolumeAnonymousBetsForMiddlesbroughToWin() throws JsonProcessingException {

        DataGenerator generator = new DataGenerator();

        DateTime startTime = DateTime.now().plusMinutes(16);
        generator.generateHighVolumeAnonymousBetsForMiddlesbroughToWin(startTime, 500, 5, 100);
    }

    //Use this test to generate the high stake betting on one selection
    //Generates two ultra-high value bets (£10,000 - £500,000) on Leeds to win within a maxiumum of 20 seconds
    @Test
    public void canGenerateUltraHighValueAnonymousBetsForLeedsToWin() throws JsonProcessingException {

        DataGenerator generator = new DataGenerator();

        DateTime startTime = DateTime.now().plusMinutes(16);
        generator.generateUltaHighValueAnonymousBetsForLeedsToWin(startTime, 10000, 5000, 2);
    }

    //Use this to generate a marker player betting sequence
    //Generate Steve Jones and Emma Green 50k and 10k, Bristol to win. 3 intervening random bets
    //All fired within 5 seconds
    @Test
    public void canGenerateMarkerPlayerBettingSequence() {

        DataGenerator generator = new DataGenerator();

        DateTime startTime = DateTime.now().plusMinutes(16);
        generator.generateMarkerPlayerBettingSequence(startTime, 1000, 500, 3);
    }
}
