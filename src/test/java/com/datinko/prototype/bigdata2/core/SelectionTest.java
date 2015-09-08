package com.datinko.prototype.bigdata2.core;

import com.datinko.prototype.bigdata2.core.factories.CustomerFactory;
import com.datinko.prototype.bigdata2.core.factories.LocationFactory;
import com.datinko.prototype.bigdata2.core.factories.MarketFactory;
import org.joda.money.Money;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * Tests to confirm the Selection class behaves as expected.
 */
public class SelectionTest {

    @Test
    public void canCreateSelection() {

        UUID id = UUID.randomUUID();
        Market market = MarketFactory.getMiddlesbroughHullToWin();
        String selectionValue = "Middlesbrough";
        String price = "2/1";

        Selection testSelection = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(selectionValue)
                .withPrice(price)
                .build();

        assertNotNull(testSelection);
        assertEquals(id, testSelection.getId());
        assertEquals(market, testSelection.getMarket());
        assertEquals(selectionValue, testSelection.getSelectionValue());
        assertEquals(price, testSelection.getPrice());
    }

    @Test
    public void canCreateSelectionWithBets() {

        UUID betId = UUID.randomUUID();
        Customer testCustomer = CustomerFactory.getBobSmith();
        Location testLocation = LocationFactory.getWHLeedsMerrion();
        Money testStake = Money.parse("GBP 20");
        DateTime timestamp = DateTime.now();

        Bet testBet = Bet.newBuilder()
                .withId(betId)
                .withCustomer(testCustomer)
                .withLocation(testLocation)
                .withStake(testStake)
                .withTimestamp(timestamp)
                .build();

        List<Bet> bets = new ArrayList<>();
        bets.add(testBet);

        UUID selectionId = UUID.randomUUID();
        Market market = MarketFactory.getMiddlesbroughHullToWin();
        String selectionValue = "Middlesbrough";
        String price = "2/1";

        Selection testSelection = Selection.newBuilder()
                .withId(selectionId)
                .withMarket(market)
                .withSelectionValue(selectionValue)
                .withPrice(price)
                .withBets(bets)
                .build();

        assertEquals(selectionId, testSelection.getId());
        assertEquals(selectionValue, testSelection.getSelectionValue());

        assertEquals(1, testSelection.getBets().size());
        assertEquals(testBet.getId(), testSelection.getBets().get(0).getId());
        assertEquals(testBet.getCustomer(), testSelection.getBets().get(0).getCustomer());
        assertEquals(testBet.getLocation(),  testSelection.getBets().get(0).getLocation());
        assertEquals(testBet.getStake(),  testSelection.getBets().get(0).getStake());
        assertEquals(testBet.getTimestamp(),  testSelection.getBets().get(0).getTimestamp());
        assertEquals(testSelection, testSelection.getBets().get(0).getSelection());
    }

    @Test
    public void canAddBetToSelection() {

        UUID betId = UUID.randomUUID();
        Customer testCustomer = CustomerFactory.getBobSmith();
        Location testLocation = LocationFactory.getWHLeedsMerrion();
        Money testStake = Money.parse("GBP 20");
        DateTime timestamp = DateTime.now();

        Bet testBet = Bet.newBuilder()
                .withId(betId)
                .withCustomer(testCustomer)
                .withLocation(testLocation)
                .withStake(testStake)
                .withTimestamp(timestamp)
                .build();

        List<Bet> bets = new ArrayList<>();
        bets.add(testBet);


        UUID selectionId = UUID.randomUUID();
        Market market = MarketFactory.getMiddlesbroughHullToWin();
        String selectionValue = "Middlesbrough";
        String price = "2/1";

        Selection testSelection = Selection.newBuilder()
                .withId(selectionId)
                .withMarket(market)
                .withSelectionValue(selectionValue)
                .withPrice(price)
                .build();

        assertEquals(selectionId, testSelection.getId());
        assertEquals(selectionValue, testSelection.getSelectionValue());
        assertEquals(price, testSelection.getPrice());
        assertEquals(0, testSelection.getBets().size());


        Selection updatedSelection = Selection.newBuilder(testSelection).withBets(bets).build();

        assertEquals(1, updatedSelection.getBets().size());
        assertEquals(testBet.getId(), updatedSelection.getBets().get(0).getId());
        assertEquals(testBet.getCustomer(), updatedSelection.getBets().get(0).getCustomer());
        assertEquals(testBet.getLocation(), updatedSelection.getBets().get(0).getLocation());
        assertEquals(testBet.getStake(), updatedSelection.getBets().get(0).getStake());
        assertEquals(testBet.getTimestamp(), updatedSelection.getBets().get(0).getTimestamp());

        assertEquals(updatedSelection, updatedSelection.getBets().get(0).getSelection());
    }

}
