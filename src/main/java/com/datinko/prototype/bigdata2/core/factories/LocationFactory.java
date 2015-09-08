package com.datinko.prototype.bigdata2.core.factories;

import com.datinko.prototype.bigdata2.core.ChannelType;
import com.datinko.prototype.bigdata2.core.Location;

import java.util.UUID;

/**
 * Created by Neil on 25/08/2015.
 */
public class LocationFactory {


    public static Location getWHLeedsMerrion() {

        UUID id = UUID.fromString("ccdc8e20-d0ae-4afe-a638-1cd8416a08f7");
        String address = "Saint John Street, Merrion St, Leeds LS2 8LQ";
        ChannelType channelType = ChannelType.RETAIL;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    public static Location getWHLeedsBoarLane() {

        UUID id = UUID.fromString("a787268e-803c-4bf0-a765-2146a0aef2a6");
        String address = "23 Boar Ln, Leeds, West Yorkshire LS1 6EA";
        ChannelType channelType = ChannelType.RETAIL;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    public static Location getWHLeedsStreetLane() {

        UUID id = UUID.fromString("315b7b16-b6ea-493a-806b-0bd93c7c1d19");
        String address = "81 Street Ln, Leeds, West Yorkshire LS8 1AP";
        ChannelType channelType = ChannelType.RETAIL;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    public static Location getWHLeedsCrossgates() {

        UUID id = UUID.fromString("a79ba50c-5c5b-44bc-8b3d-4d8b706b24d1");
        String address = "H-K Crossgates Shopping Centre, Leeds LS15 8EU";
        ChannelType channelType = ChannelType.RETAIL;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    public static Location getWHLeedsPrimroseLane() {

        UUID id = UUID.fromString("e6e89876-882c-462e-a5eb-5cada4d7c662");
        String address = "1 Primrose Lane, Halton, Leeds LS15 7HS";
        ChannelType channelType = ChannelType.RETAIL;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    public static Location getWHAnonymousOnline() {

        UUID id = UUID.fromString("aaba6e94-c66b-4f91-8969-9562d5eab101");
        String address = "10.162.158.3";
        ChannelType channelType = ChannelType.ONLINE;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    public static Location getWHAnonymousMobile() {

        UUID id = UUID.fromString("91eef273-7b2f-4a88-ad89-db94b8902791");
        String address = "10.168.132.8";
        ChannelType channelType = ChannelType.MOBILE;

        Location location = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        return location;
    }

    //TODO: Add user specific internet locations (for some)
    //TODO: Add user specific mobile locations (for some)
}
