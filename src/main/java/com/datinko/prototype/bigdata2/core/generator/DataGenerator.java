package com.datinko.prototype.bigdata2.core.generator;

import com.datinko.prototype.bigdata2.core.Bet;
import com.datinko.prototype.bigdata2.core.factories.BetFactory;
import com.datinko.prototype.bigdata2.core.factories.random.RandomBetFactory;
import com.datinko.prototype.bigdata2.core.serializer.MoneyDeserializer;
import com.datinko.prototype.bigdata2.core.serializer.MoneySerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.money.Money;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by neild on 05/09/2015.
 */
public class DataGenerator {

    protected ObjectMapper mapper = new ObjectMapper();
    protected RandomBetFactory randomBetFactory = new RandomBetFactory();
    protected Random rand = new Random();


    public DataGenerator() {

        mapper.registerModule(new JodaModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Money.class, new MoneyDeserializer());
        module.addSerializer(Money.class, new MoneySerializer());
        mapper.registerModule(module);
    }

    public List<Bet> generateTotallyRandomBets(int numberOfBetsToGenerate, int maxMsBetSpacing, int minMsBetSpacing) throws JsonProcessingException {

        Random rand = new Random();
        DateTime timestamp = DateTime.now();

        List<Bet> bets = new ArrayList<>();
        for (int i = 0; i < numberOfBetsToGenerate; i++) {
            int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
            timestamp = timestamp.plusMillis(millisecondBetSpacing);
            Bet bet = randomBetFactory.generateRandomBet(timestamp);
            bets.add(bet);
            System.out.println(mapper.writeValueAsString(bet));
        }

        return bets;
    }


    public List<Bet> generatePseudoRandomAnonymousBetsInCSV(DateTime startTime, int numberOfBetsToGenerate, int maxMsBetSpacing, int minMsBetSpacing) throws JsonProcessingException {

        DateTime timestamp = startTime;

        //we use this to weight the number of bets of each type created:
        // 60% anonymous retail
        // 25% anonymous online or mobile
        // 15% identified customer bets  (will probably used identified customers as our scenario data)
        int anonymousRetailPercentage = 70;
        int anonymousOnlineAndMobilePercentage = 100;
        //int identifiedCustomer = 100;
        List<Bet> bets = new ArrayList<>();

        File file = new File("c:\\randombetdata.txt");
        Writer fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {

            fileWriter = new FileWriter(file);
            bufferedWriter = new BufferedWriter(fileWriter);


            for (int i = 0; i < numberOfBetsToGenerate; i++) {
                Bet bet = null;

                int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
                timestamp = timestamp.plusMillis(millisecondBetSpacing);

                int typeOfBet = rand.nextInt(100);
                if (typeOfBet <= anonymousRetailPercentage) {
                    bet = randomBetFactory.generateAnonymousRetailRandomBet(timestamp);
                } else if (typeOfBet <= anonymousOnlineAndMobilePercentage) {
                    bet = randomBetFactory.generateAnonymousOnlineOrMobileRandomBet(timestamp);
                }
                //else if (typeOfBet <= identifiedCustomer) {
                //                bet = randomBetFactory.generateRandomBet(timestamp);
                //            }

                bets.add(bet);

                //System.out.println(mapper.writeValueAsString(bet));
                System.out.println(bet.toString());


                bufferedWriter.write(bet.toString());
                bufferedWriter.newLine();
            }


        } catch (IOException e) {
            System.err.println("Error writing the file : ");
            e.printStackTrace();
        } finally {

            if (bufferedWriter != null && fileWriter != null) {
                try {
                    bufferedWriter.close();
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bets;
    }

    public List<Bet> generatePseudoRandomAnonymousBetsInJSON(DateTime startTime, int numberOfBetsToGenerate, int maxMsBetSpacing, int minMsBetSpacing) throws JsonProcessingException {

        DateTime timestamp = startTime;

        //we use this to weight the number of bets of each type created:
        // 60% anonymous retail
        // 25% anonymous online or mobile
        // 15% identified customer bets  (will probably used identified customers as our scenario data)
        int anonymousRetailPercentage = 70;
        int anonymousOnlineAndMobilePercentage = 100;
        //int identifiedCustomer = 100;
        List<Bet> bets = new ArrayList<>();

        File file = new File("c:\\randombetdata.json");
        Writer fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {

            fileWriter = new FileWriter(file);
            bufferedWriter = new BufferedWriter(fileWriter);


            for (int i = 0; i < numberOfBetsToGenerate; i++) {
                Bet bet = null;

                int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
                timestamp = timestamp.plusMillis(millisecondBetSpacing);

                int typeOfBet = rand.nextInt(100);
                if (typeOfBet <= anonymousRetailPercentage) {
                    bet = randomBetFactory.generateAnonymousRetailRandomBet(timestamp);
                } else if (typeOfBet <= anonymousOnlineAndMobilePercentage) {
                    bet = randomBetFactory.generateAnonymousOnlineOrMobileRandomBet(timestamp);
                }
                //else if (typeOfBet <= identifiedCustomer) {
                //                bet = randomBetFactory.generateRandomBet(timestamp);
                //            }

                bets.add(bet);

                System.out.println(mapper.writeValueAsString(bet));
                //System.out.println(bet.toString());


                bufferedWriter.write(mapper.writeValueAsString(bet));
                bufferedWriter.newLine();
            }


        } catch (IOException e) {
            System.err.println("Error writing the file : ");
            e.printStackTrace();
        } finally {

            if (bufferedWriter != null && fileWriter != null) {
                try {
                    bufferedWriter.close();
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bets;
    }

    public List<Bet> generateRandomKnownCustomerHighValueBets(int numberOfBetsToGenerate, int maxMsBetSpacing, int minMsBetSpacing) throws JsonProcessingException {

        DateTime timestamp = DateTime.now();

        List<Bet> bets = new ArrayList<>();

        for (int i = 0; i < numberOfBetsToGenerate; i++) {
            int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
            timestamp = timestamp.plusMillis(millisecondBetSpacing);

            Bet bet = randomBetFactory.generateKnownRandomCustomerHighValueBet(timestamp);

            bets.add(bet);
            System.out.println(mapper.writeValueAsString(bet));
        }
        return bets;
    }

    public List<Bet> generateHighVolumeAnonymousBetsForMiddlesbroughToWin(DateTime startTime, int maxMsBetSpacing, int minMsBetSpacing, int numberOfBetsToGenerate) {

        DateTime timestamp = startTime;
        List<Bet> bets = new ArrayList<>();

        for (int i = 0; i < numberOfBetsToGenerate; i++) {
            int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
            timestamp = timestamp.plusMillis(millisecondBetSpacing);

            Bet bet = randomBetFactory.generateRandomAnonymousLowValueBetOnMiddlesbroughToWin(timestamp);

            bets.add(bet);
            System.out.println(bet.toString());
        }
        return bets;
    }

    public List<Bet> generateUltaHighValueAnonymousBetsForLeedsToWin(DateTime startTime, int maxMsBetSpacing, int minMsBetSpacing, int numberOfBetsToGenerate) {

        DateTime timestamp = startTime;
        List<Bet> bets = new ArrayList<>();

        for (int i = 0; i < numberOfBetsToGenerate; i++) {
            int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
            timestamp = timestamp.plusMillis(millisecondBetSpacing);

            Bet bet = randomBetFactory.generateRandomAnonymousUltraHighValueBetOnLeedsToWin(timestamp);

            bets.add(bet);
            System.out.println(bet.toString());
        }
        return bets;
    }


    public List<Bet> generateMarkerPlayerBettingSequence(DateTime startTime, int maxMsBetSpacing, int minMsBetSpacing, int numberOfBetsToGenerate) {

        DateTime timestamp = startTime;
        List<Bet> bets = new ArrayList<>();

        //generate first marker player bet
        Bet bet = BetFactory.getSteveJonesBetting50KOnlineForBristolToWin(startTime);
        bets.add(bet);
        System.out.println(bet.toString());

        //generate 'filler' bets
        for (int i = 0; i < numberOfBetsToGenerate; i++) {
            int millisecondBetSpacing = rand.nextInt(maxMsBetSpacing) + minMsBetSpacing;
            timestamp = timestamp.plusMillis(millisecondBetSpacing);

            bet = randomBetFactory.generateRandomAnonymousLowValueBet(timestamp);

            bets.add(bet);
            System.out.println(bet.toString());
        }

        //generate second marker player bet
        bet = BetFactory.getEmmaGreenBetting10KOnlineForBristolToWin(startTime);
        bets.add(bet);
        System.out.println(bet.toString());

        return bets;
    }
}
