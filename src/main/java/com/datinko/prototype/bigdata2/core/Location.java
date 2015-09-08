package com.datinko.prototype.bigdata2.core;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.UUID;

/**
 * Simple representation of where a bet was placed from.
 * (Shop, Internet IP Address or Mobile application IP address)
 */
@JsonDeserialize(builder=Location.Builder.class)
public class Location {

    protected final UUID id;
    protected final String address;
    protected final ChannelType channel;

    private Location(Builder builder) {
        id = builder.id;
        address = builder.address;
        channel = builder.channel;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Location copy) {
        Builder builder = new Builder();
        builder.id = copy.id;
        builder.address = copy.address;
        builder.channel = copy.channel;
        return builder;
    }

    public UUID getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public ChannelType getChannel() {
        return channel;
    }


    public static final class Builder {

        private UUID id;
        private String address;
        private ChannelType channel;

        private Builder() {
        }

        public Builder withId(UUID val) {
            id = val;
            return this;
        }

        public Builder withAddress(String val) {
            address = val;
            return this;
        }

        public Builder withChannel(ChannelType val) {
            channel = val;
            return this;
        }

        public Location build() {
            return new Location(this);
        }
    }
}
