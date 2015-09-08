package com.datinko.prototype.bigdata2.core.factories.random;

import com.datinko.prototype.bigdata2.core.Location;
import com.datinko.prototype.bigdata2.core.factories.LocationFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by neild on 05/09/2015.
 */
public class RandomLocationFactory {

    protected List<Location> locations = new ArrayList<>();
    protected List<Location> onlineAndMobileLocations = new ArrayList<>();
    protected List<Location> retailLocations = new ArrayList<>();
    protected Random rand = new Random();

    public RandomLocationFactory() {
        this.locations.add(LocationFactory.getWHAnonymousMobile());
        this.locations.add(LocationFactory.getWHAnonymousOnline());
        this.locations.add(LocationFactory.getWHLeedsBoarLane());
        this.locations.add(LocationFactory.getWHLeedsCrossgates());
        this.locations.add(LocationFactory.getWHLeedsMerrion());
        this.locations.add(LocationFactory.getWHLeedsPrimroseLane());
        this.locations.add(LocationFactory.getWHLeedsStreetLane());

        this.onlineAndMobileLocations.add(LocationFactory.getWHAnonymousMobile());
        this.onlineAndMobileLocations.add(LocationFactory.getWHAnonymousOnline());

        this.retailLocations.add(LocationFactory.getWHLeedsBoarLane());
        this.retailLocations.add(LocationFactory.getWHLeedsCrossgates());
        this.retailLocations.add(LocationFactory.getWHLeedsMerrion());
        this.retailLocations.add(LocationFactory.getWHLeedsPrimroseLane());
        this.retailLocations.add(LocationFactory.getWHLeedsStreetLane());

    }

    public Location getRandomLocation() {

        int index = rand.nextInt(locations.size() - 1);
        return locations.get(index);
    }

    public Location getRandomOnlineOrMobileLocation() {

        int index = rand.nextInt(onlineAndMobileLocations.size() - 1);
        return onlineAndMobileLocations.get(index);
    }

    public Location getRandomRetailLocation() {

        int index = rand.nextInt(retailLocations.size() - 1);
        return retailLocations.get(index);
    }
}
