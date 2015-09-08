package com.datinko.prototype.bigdata2.core.factories;


import com.datinko.prototype.bigdata2.core.Customer;

import java.util.UUID;

/**
 * Created by Neil on 25/08/2015.
 */
public class CustomerFactory {

    public static Customer getAnonymous() {

        UUID id = UUID.fromString(UUID.randomUUID().toString());
        String name = "Anonymous";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getBobSmith() {

        UUID id = UUID.fromString("caff853f-3a50-412c-a5fa-23551c22b3fd");
        String name = "Bob Smith";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getSteveJones() {

        UUID id = UUID.fromString("e5aa503c-4b8a-4e54-a4d0-f364b2c6c1b5");
        String name = "Steve Jones";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getEricSmith() {

        UUID id = UUID.fromString("b467ca8f-c869-410e-ab1b-f7b1a87cccd5");
        String name = "Eric Smith";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getEmmaGreen() {

        UUID id = UUID.fromString("e680dbfe-ca65-4904-aa6d-5f239c1ccc0b");
        String name = "Emma Green";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getSarahWhite() {

        UUID id = UUID.fromString("4ad3b22a-3fbc-4c83-aed8-0b71b8682a35");
        String name = "Sarah White";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getTonyGold() {

        UUID id = UUID.fromString("03ff85a6-c276-4b72-9788-2acf3fd0107f");
        String name = "Tony Gold";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getEveWhitworth() {

        UUID id = UUID.fromString("0ee0200c-72a5-4a0d-a7c4-cde4a2d91025");
        String name = "Eve Whitworth";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getColinBladen() {

        UUID id = UUID.fromString("3417c91b-23b7-45e5-bd0e-b07d3d5b2ad5");
        String name = "Colin Bladen";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getPeteRose() {

        UUID id = UUID.fromString("a223a6e6-b4ad-4424-b4ba-b225f1902f5b");
        String name = "Pete Rose";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }

    public static Customer getAmyBrown() {

        UUID id = UUID.fromString("c97393dd-06cd-4467-bd71-aecffa586377");
        String name = "Amy Brown";

        Customer customer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        return customer;
    }
}
