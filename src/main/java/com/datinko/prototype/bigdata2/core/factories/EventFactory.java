package com.datinko.prototype.bigdata2.core.factories;


import com.datinko.prototype.bigdata2.core.Event;

import java.util.UUID;

/**
 * Created by neild on 26/08/2015.
 */
public class EventFactory {

    public static Event getMiddlesbroughVsHull() {

        UUID id = UUID.fromString("709da48e-9bfa-4255-afda-bb00725958d8");
        String name = "Middlesbrough Vs Hull";

        Event event = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return event;
    }

    public static Event getLeedsVsBlackburn() {

        UUID id = UUID.fromString("b5aedc18-4d56-453a-b854-3043a3eae00e");
        String name = "Leeds Vs Blackburn";

        Event event = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return event;
    }

    public static Event getChartonVsBurnley() {

        UUID id = UUID.fromString("f4e07b5c-8cf8-4254-a4aa-b4d49c165925");
        String name = "Charlton Vs Burnley";

        Event event = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return event;
    }

    public static Event getRotherhamVsBolton() {

        UUID id = UUID.fromString("86f141d5-a93a-4945-a018-610cbe16ea55");
        String name = "Rotherham Vs Bolton";

        Event event = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return event;
    }

    public static Event getWolvesVsReading() {

        UUID id = UUID.fromString("5e603b81-f822-4147-8b55-b6e92f186e95");
        String name = "Wolves Vs Reading";

        Event event = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return event;
    }

    public static Event getBristolVsDerby() {

        UUID id = UUID.fromString("9ac50321-233a-435e-83cc-18b7df416ecd");
        String name = "Bristol Vs Derby";

        Event event = Event.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return event;
    }
}
