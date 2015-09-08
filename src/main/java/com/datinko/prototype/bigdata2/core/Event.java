package com.datinko.prototype.bigdata2.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Simple representation of an Event.
 */
@JsonDeserialize(builder=Event.Builder.class)
public class Event {

    protected final UUID id;
    protected final String name;
    //@JsonProperty("markets")
    @JsonIgnore
    protected List<Market> markets;

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @JsonIgnore
    public List<Market> getMarkets() {
        return Collections.unmodifiableList(markets);
    }

    private Event(Builder builder) {
        id = builder.id;
        name = builder.name;
        markets = builder.markets;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Event copy) {
        Builder builder = new Builder();
        builder.id = copy.id;
        builder.name = copy.name;
        builder.markets = copy.markets;
        return builder;
    }


    public static final class Builder {
        private UUID id;
        private String name;
        private List<Market> markets = new ArrayList<>();

        private Builder() {
        }

        public Builder withId(UUID val) {
            id = val;
            return this;
        }

        public Builder withName(String val) {
            name = val;
            return this;
        }

        public Builder withMarkets(List<Market> val) {
            markets = val;

            return this;
        }

        public Event build() {
            Event event = new Event(this);
            List<Market> updatedMarkets = new ArrayList<>();
            for(Market market : markets) {
                market = Market.newBuilder(market).withEvent(event).build();
                updatedMarkets.add(market);
            }
            event.markets = updatedMarkets;
            return event;
        }
    }
}
