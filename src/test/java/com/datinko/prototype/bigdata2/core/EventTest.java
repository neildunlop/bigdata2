package com.datinko.prototype.bigdata2.core;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Tests to confirm the Event class behaves as expected.
 */
public class EventTest {

    @Test
    public void canCreateEvent() {

        UUID id = UUID.randomUUID();
        String name = "Middlesbrough Vs Leeds";

        Event testEvent = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        assertNotNull(testEvent);
        assertEquals(id, testEvent.getId());
        assertEquals(name, testEvent.getName());
    }

    @Test
    public void canCreateEventWithMarkets() {
        UUID id = UUID.randomUUID();
        String name = "To Win";
        Market testMarket = Market.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        List<Market> markets = new ArrayList<>();
        markets.add(testMarket);

        Event testEvent = Event.newBuilder()
                .withId(id)
                .withName(name)
                .withMarkets(markets)
                .build();

        assertEquals(id, testEvent.getId());
        assertEquals(name, testEvent.getName());

        assertEquals(1, testEvent.getMarkets().size());
        assertEquals(testMarket.getId(), testEvent.getMarkets().get(0).getId());
        assertEquals(testMarket.getName(), testEvent.getMarkets().get(0).getName());
        assertEquals(testMarket.getSelections(), testEvent.getMarkets().get(0).getSelections());
        assertEquals(testEvent, testEvent.getMarkets().get(0).getEvent());
    }

    @Test
    public void canAddMarketToEvent() {

        UUID id = UUID.randomUUID();
        String name = "To Win";
        Market testMarket = Market.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        List<Market> markets = new ArrayList<>();
        markets.add(testMarket);

        Event testEvent = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        assertEquals(id, testEvent.getId());
        assertEquals(name, testEvent.getName());
        assertEquals(0, testEvent.getMarkets().size());

        Event updatedEvent = Event.newBuilder(testEvent).withMarkets(markets).build();

        assertEquals(1, updatedEvent.getMarkets().size());
        assertEquals(testMarket.getId(), updatedEvent.getMarkets().get(0).getId());
        assertEquals(testMarket.getName(), updatedEvent.getMarkets().get(0).getName());
        assertEquals(testMarket.getSelections(), updatedEvent.getMarkets().get(0).getSelections());
        assertEquals(updatedEvent, updatedEvent.getMarkets().get(0).getEvent());
    }

    @Test
    public void canSerializeEvent() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        UUID id = UUID.randomUUID();
        String name = "Middlesbrough Vs Newcastle";
        Market testMarket = Market.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        List<Market> markets = new ArrayList<>();
        markets.add(testMarket);

        Event testEvent = Event.newBuilder()
                .withId(id)
                .withName(name)
                .withMarkets(markets)
                .build();

        String result = mapper.writeValueAsString(testEvent);

        assertNotNull(result);


    }

}
