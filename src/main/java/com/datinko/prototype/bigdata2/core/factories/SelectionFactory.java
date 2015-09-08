package com.datinko.prototype.bigdata2.core.factories;


import com.datinko.prototype.bigdata2.core.Market;
import com.datinko.prototype.bigdata2.core.Selection;

import java.util.UUID;

/**
 * Created by Neil on 25/08/2015.
 */
public class SelectionFactory {

    public static Selection getBristolToWin() {

        UUID id = UUID.fromString("5bc5be63-de7e-4304-bb56-bebbda60c060");
        EventFactory.getBristolVsDerby();
        Market market = MarketFactory.getBristolDerbyToWin();
        String price = "8/5";
        String name = "Bristol";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getDerbyToWin() {

        UUID id = UUID.fromString("3b10cbc1-d79c-4186-b60e-39c6462f2374");
        EventFactory.getBristolVsDerby();
        Market market = MarketFactory.getBristolDerbyToWin();
        String price = "15/8";
        String name = "Derby";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getBristolDerbyToDraw() {

        UUID id = UUID.fromString("d9dd89d7-bd0c-4618-ad7e-800ae4883ef9");
        EventFactory.getBristolVsDerby();
        Market market = MarketFactory.getBristolDerbyToDraw();
        String price = "21/8";
        String name = "Draw";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getMiddlesbroughToWin() {

        UUID id = UUID.fromString("a4e7ea00-7514-4a08-98b0-0ee7d389cc9d");
        EventFactory.getMiddlesbroughVsHull();
        Market market = MarketFactory.getMiddlesbroughHullToWin();
        String price = "5/4";
        String name = "Middlesbrough";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getHullToWin() {

        UUID id = UUID.fromString("7f4c18e3-16e8-47d5-804b-9b4aa6deb318");
        EventFactory.getMiddlesbroughVsHull();
        Market market = MarketFactory.getMiddlesbroughHullToWin();
        String price = "11/5";
        String name = "Hull";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getMiddlesbroughHullToDraw() {

        UUID id = UUID.fromString("7d0cc327-cd8e-494e-908f-c1bb9da1e298");
        EventFactory.getMiddlesbroughVsHull();
        Market market = MarketFactory.getMiddlesbroughHullToDraw();
        String price = "23/10";
        String name = "Draw";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getLeedsToWin() {

        UUID id = UUID.fromString("9e8239bb-7ceb-4334-b300-b9dd7ecb80df");
        EventFactory.getLeedsVsBlackburn();
        Market market = MarketFactory.getLeedsBlackburnToWin();
        String price = "12/5";
        String name = "Leeds";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getBlackburnToWin() {

        UUID id = UUID.fromString("c58cc59c-9ce1-4ad1-8b12-99f6537ef080");
        EventFactory.getLeedsVsBlackburn();
        Market market = MarketFactory.getLeedsBlackburnToWin();
        String price = "23/20";
        String name = "Blackburn";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getLeedsBlackburnToDraw() {

        UUID id = UUID.fromString("af76f513-f149-4c2a-904f-82b90dc67800");
        EventFactory.getLeedsVsBlackburn();
        Market market = MarketFactory.getLeedsBlackburnToDraw();
        String price = "23/10";
        String name = "Draw";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getCharltonToWin() {

        UUID id = UUID.fromString("a5460227-186f-4733-a2fe-521c4da2e66c");
        EventFactory.getChartonVsBurnley();
        Market market = MarketFactory.getCharltonBurnleyToWin();
        String price = "5/6";
        String name = "Charlton";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getBurnleyToWin() {

        UUID id = UUID.fromString("9793bc47-5564-4692-957b-57d8407597b5");
        EventFactory.getChartonVsBurnley();
        Market market = MarketFactory.getCharltonBurnleyToWin();
        String price = "15/4";
        String name = "Burnley";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getCharltonBurnleyToDraw() {

        UUID id = UUID.fromString("b36d1151-e860-4fc3-a499-ff36d672d9ea");
        EventFactory.getChartonVsBurnley();
        Market market = MarketFactory.getCharltonBurnleyToDraw();
        String price = "23/10";
        String name = "Draw";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getRotherhamToWin() {

        UUID id = UUID.fromString("04eca3e0-35df-4d95-a8ac-9dee70113241");
        EventFactory.getRotherhamVsBolton();
        Market market = MarketFactory.getRotherhamBoltonToWin();
        String price = "12/1";
        String name = "Rotherham";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getBoltonToWin() {

        UUID id = UUID.fromString("973202cd-da84-4112-9043-ef5175dc65e8");
        EventFactory.getBristolVsDerby();
        Market market = MarketFactory.getRotherhamBoltonToWin();
        String price = "1/4";
        String name = "Bolton";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getRotherhamBoltonToDraw() {

        UUID id = UUID.fromString("39cd368b-3c43-454e-a139-188fc54591f0");
        EventFactory.getRotherhamVsBolton();
        Market market = MarketFactory.getRotherhamBoltonToDraw();
        String price = "9/2";
        String name = "Draw";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getWolvesToWin() {

        UUID id = UUID.fromString("83f5c195-6c8d-4db9-918d-3af23ebaed9e");
        EventFactory.getWolvesVsReading();
        Market market = MarketFactory.getWolvesReadingToWin();
        String price = "2/5";
        String name = "Wolves";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getReadingToWin() {

        UUID id = UUID.fromString("0dbf73a5-64db-4c45-b686-f0169f4ab060");
        EventFactory.getWolvesVsReading();
        Market market = MarketFactory.getWolvesReadingToWin();
        String price = "17/2";
        String name = "Reading";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }

    public static Selection getWolvesReadingToDraw() {

        UUID id = UUID.fromString("61433299-19ad-4ef9-bc4b-6f3e64bdfb37");
        EventFactory.getWolvesVsReading();
        Market market = MarketFactory.getWolvesReadingToDraw();
        String price = "16/5";
        String name = "Draw";

        Selection result = Selection.newBuilder()
                .withId(id)
                .withMarket(market)
                .withSelectionValue(name)
                .withPrice(price)
                .build();

        return result;
    }


}
