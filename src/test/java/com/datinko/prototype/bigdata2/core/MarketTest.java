package com.datinko.prototype.bigdata2.core;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datinko.prototype.bigdata2.core.factories.EventFactory;
import org.junit.Test;

/**
 * Created by neild on 26/08/2015.
 */
public class MarketTest {

    @Test
    public void canCreateMarket() {

        UUID id = UUID.randomUUID();
        Event event = EventFactory.getMiddlesbroughVsHull();
        String name = "To Win";

        Market testMarket = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                //no selections on this one
                .build();

        assertNotNull(testMarket);
        assertEquals(id, testMarket.getId());
        assertEquals(event, testMarket.getEvent());
        assertEquals(name, testMarket.getName());
    }

    @Test
    public void canCreateMarketWithSelections() {

        UUID id = UUID.randomUUID();
        String name = "To Win";
        String testPrice = "2/1";

        Selection testSelection = Selection.newBuilder()
                .withId(id)
                .withSelectionValue(name)
                .withPrice(testPrice)
                .build();

        List<Selection> selections = new ArrayList<>();
        selections.add(testSelection);

        Market testMarket = Market.newBuilder()
                .withId(id)
                .withName(name)
                .withSelections(selections)
                .build();

        assertEquals(id, testMarket.getId());
        assertEquals(name, testMarket.getName());

        assertEquals(1, testMarket.getSelections().size());
        assertEquals(testSelection.getId(), testMarket.getSelections().get(0).getId());
        assertEquals(testSelection.getSelectionValue(), testMarket.getSelections().get(0).getSelectionValue());
        assertEquals(testSelection.getBets(), testMarket.getSelections().get(0).getBets());
        assertEquals(testMarket, testMarket.getSelections().get(0).getMarket());
    }

    @Test
    public void canAddSelectionToMarket() {

        UUID id = UUID.randomUUID();
        String selectionValue = "Middlesbrough";
        Selection testSelection = Selection.newBuilder()
                .withId(id)
                .withSelectionValue(selectionValue)
                .build();

        List<Selection> selections = new ArrayList<>();
        selections.add(testSelection);

        String marketName = "To Win";
        Market testMarket = Market.newBuilder()
                .withId(id)
                .withName(marketName)
                .build();

        assertEquals(id, testMarket.getId());
        assertEquals(marketName, testMarket.getName());
        assertEquals(0, testMarket.getSelections().size());

        Market updatedMarket = Market.newBuilder(testMarket).withSelections(selections).build();

        assertEquals(1, updatedMarket.getSelections().size());
        assertEquals(testSelection.getId(), updatedMarket.getSelections().get(0).getId());
        assertEquals(testSelection.getSelectionValue(), updatedMarket.getSelections().get(0).getSelectionValue());
        assertEquals(testSelection.getPrice(), updatedMarket.getSelections().get(0).getPrice());

        assertEquals(updatedMarket, updatedMarket.getSelections().get(0).getMarket());
    }
}
