package com.datinko.prototype.bigdata2.core.factories;


import com.datinko.prototype.bigdata2.core.Event;
import com.datinko.prototype.bigdata2.core.Market;

import java.util.UUID;

public class MarketFactory {

    public static Market getBristolDerbyToWin() {

        UUID id = UUID.fromString("6bc5be74-af9e-4405-cc56-bebbda60c061");
        Event event = EventFactory.getBristolVsDerby();
        String name = "To Win";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                //no selections for now
                .build();

        return result;
    }

    public static Market getBristolDerbyToDraw() {

        UUID id = UUID.fromString("64bcec4c-8115-4979-9c7f-fb3c5da0f3c6");
        Event event = EventFactory.getBristolVsDerby();
        String name = "To Draw";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getMiddlesbroughHullToWin() {

        UUID id = UUID.fromString("13478342-688a-4bcb-a305-3f990f8f2c85");
        Event event = EventFactory.getMiddlesbroughVsHull();
        String name = "To Win";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getMiddlesbroughHullToDraw() {

        UUID id = UUID.fromString("667beb72-bc94-4783-a355-9af78b8a665a");
        Event event = EventFactory.getMiddlesbroughVsHull();
        String name = "To Draw";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getLeedsBlackburnToWin() {

        UUID id = UUID.fromString("708ea954-6b6f-4a0e-9862-82292ad9bd09");
        Event event = EventFactory.getLeedsVsBlackburn();
        String name = "To Win";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getLeedsBlackburnToDraw() {

        UUID id = UUID.fromString("1e0c855b-7d6f-4de7-a6e2-e18d750e6665");
        Event event = EventFactory.getLeedsVsBlackburn();
        String name = "To Draw";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getCharltonBurnleyToWin() {

        UUID id = UUID.fromString("cd310dc0-d49e-4189-a103-b089229311e9");
        Event event = EventFactory.getChartonVsBurnley();
        String name = "To Win";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getCharltonBurnleyToDraw() {

        UUID id = UUID.fromString("911fa39f-970c-4be0-9cfd-a6d5a15d4651");
        Event event = EventFactory.getChartonVsBurnley();
        String name = "To Draw";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getRotherhamBoltonToWin() {

        UUID id = UUID.fromString("1f60948e-3369-4f98-a7bc-7f9f8ea1b13f");
        Event event = EventFactory.getRotherhamVsBolton();
        String name = "To Win";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getRotherhamBoltonToDraw() {

        UUID id = UUID.fromString("8a44727a-cfb9-41fd-86fc-1f0a114086d3");
        Event event = EventFactory.getRotherhamVsBolton();
        String name = "To Draw";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getWolvesReadingToWin() {

        UUID id = UUID.fromString("63561133-c2aa-409c-afc6-1b810bef03d5");
        Event event = EventFactory.getWolvesVsReading();
        String name = "To Win";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;
    }

    public static Market getWolvesReadingToDraw() {

        UUID id = UUID.fromString("fd58e3c5-5b7a-4a84-b687-a29732a63d1b");
        Event event = EventFactory.getWolvesVsReading();
        String name = "To Draw";

        Market result = Market.newBuilder()
                .withId(id)
                .withEvent(event)
                .withName(name)
                        //no selections for now
                .build();

        return result;

    }
}
