package com.datinko.prototype.bigdata2.core;

import com.datinko.prototype.bigdata2.core.factories.CustomerFactory;
import com.datinko.prototype.bigdata2.core.factories.LocationFactory;
import com.datinko.prototype.bigdata2.core.factories.SelectionFactory;
import com.datinko.prototype.bigdata2.core.serializer.MoneyDeserializer;
import com.datinko.prototype.bigdata2.core.serializer.MoneySerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.money.Money;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * Tests to confirm the Bet class functions as expected.
 */
public class BetTest {

    @Test
    public void canCreateBet() {

        UUID id = UUID.randomUUID();
        Customer testCustomer = CustomerFactory.getBobSmith();
        Location testLocation = LocationFactory.getWHLeedsMerrion();
        Selection testSelection = SelectionFactory.getBristolToWin();
        Money testStake = Money.parse("GBP 20");
        DateTime timestamp = DateTime.now();

        Bet testBet = Bet.newBuilder()
                .withId(id)
                .withCustomer(testCustomer)
                .withLocation(testLocation)
                .withSelection(testSelection)
                .withStake(testStake)
                .withTimestamp(timestamp)
                .build();

        assertNotNull(testBet);
        assertEquals(id, testBet.getId());
        assertEquals(testCustomer, testBet.getCustomer());
        assertEquals(testLocation, testBet.getLocation());
        assertEquals(testSelection, testBet.getSelection());
        assertEquals(testStake, testBet.getStake());
        assertEquals(timestamp, testBet.getTimestamp());
    }

    @Test
    public void canModifyAnExistingBet() {

        UUID id = UUID.randomUUID();
        Customer testCustomer = CustomerFactory.getBobSmith();
        Location testLocation = LocationFactory.getWHLeedsMerrion();
        Selection testSelection = SelectionFactory.getDerbyToWin();
        Money testStake1 = Money.parse("GBP 20");
        Money testStake2 = Money.parse("GBP 30");
        DateTime timestamp = DateTime.now();

        Bet testBet = Bet.newBuilder()
                .withId(id)
                .withCustomer(testCustomer)
                .withLocation(testLocation)
                .withSelection(testSelection)
                .withStake(testStake1)
                .withTimestamp(timestamp)
                .build();

        assertNotNull(testBet);
        assertEquals(id, testBet.getId());
        assertEquals(testCustomer, testBet.getCustomer());
        assertEquals(testLocation, testBet.getLocation());
        assertEquals(testSelection, testBet.getSelection());
        assertEquals(testStake1, testBet.getStake());
        assertEquals(timestamp, testBet.getTimestamp());

        Bet updatedBet = Bet.newBuilder(testBet)
                .withStake(testStake2)
                .build();

        assertNotNull(updatedBet);
        assertEquals(id, updatedBet.getId());
        assertEquals(testCustomer, updatedBet.getCustomer());
        assertEquals(testLocation, updatedBet.getLocation());
        assertEquals(testSelection, updatedBet.getSelection());
        assertEquals(testStake2, updatedBet.getStake());
        assertEquals(timestamp, updatedBet.getTimestamp());
    }

    @Ignore
    public void canSerializeBet() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Money.class, new MoneyDeserializer());
        module.addSerializer(Money.class, new MoneySerializer());
        mapper.registerModule(module);

        UUID id = UUID.fromString("c84a7630-8e06-4996-8adb-a2632dd2aa9e");
        Customer testCustomer = CustomerFactory.getBobSmith();
        Location testLocation = LocationFactory.getWHLeedsMerrion();
        Selection testSelection = SelectionFactory.getMiddlesbroughToWin();
        Money testStake = Money.parse("GBP 20");
        DateTime timestamp = DateTime.now();

        String expectedJson = "{\"id\":\"c84a7630-8e06-4996-8adb-a2632dd2aa9e\",\"timestamp\":\"2015-09-05T19:35:58.724Z\",\"customer\":{\"id\":\"caff853f-3a50-412c-a5fa-23551c22b3fd\",\"name\":\"Bob Smith\"},\"location\":{\"id\":\"ccdc8e20-d0ae-4afe-a638-1cd8416a08f7\",\"address\":\"Saint John Street, Merrion St, Leeds LS2 8LQ\",\"channel\":\"RETAIL\"},\"selection\":{\"id\":\"a4e7ea00-7514-4a08-98b0-0ee7d389cc9d\",\"selectionValue\":\"Middlesbrough\",\"price\":\"2/1\",\"market\":{\"id\":\"13478342-688a-4bcb-a305-3f990f8f2c85\",\"name\":\"To Win\",\"event\":{\"id\":\"709da48e-9bfa-4255-afda-bb00725958d8\",\"name\":\"Middlesbrough Vs Hull\"}}},\"stake\":\"GBP 20.00\"}";

        Bet testBet = Bet.newBuilder()
                .withId(id)
                .withCustomer(testCustomer)
                .withLocation(testLocation)
                .withSelection(testSelection)
                .withStake(testStake)
                .withTimestamp(timestamp)
                .build();

        String result = mapper.writeValueAsString(testBet);

        assertNotNull(result);
        assertEquals(expectedJson, result);

    }

}
