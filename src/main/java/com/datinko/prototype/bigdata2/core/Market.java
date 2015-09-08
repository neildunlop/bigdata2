package com.datinko.prototype.bigdata2.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Simple representation of a Market.
 */
@JsonDeserialize(builder=Market.Builder.class)
public class Market {

    protected final UUID id;
    protected final String name;
    protected final Event event;

    //@JsonProperty("selections")
    @JsonIgnore
    protected List<Selection> selections;

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Event getEvent() {
        return event;
    }

    @JsonIgnore
    public List<Selection> getSelections() {
        return Collections.unmodifiableList(selections);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Market market = (Market) o;

        if (!id.equals(market.id)) return false;
        if (name != null ? !name.equals(market.name) : market.name != null) return false;
        if (event != null ? !event.equals(market.event) : market.event != null) return false;
        return !(selections != null ? !selections.equals(market.selections) : market.selections != null);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (event != null ? event.hashCode() : 0);
        result = 31 * result + (selections != null ? selections.hashCode() : 0);
        return result;
    }

    private Market(Builder builder) {
        id = builder.id;
        name = builder.name;
        event = builder.event;
        selections = builder.selections;
    }



    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Market copy) {
        Builder builder = new Builder();
        builder.id = copy.id;
        builder.name = copy.name;
        builder.event = copy.event;
        builder.selections = copy.selections;
        return builder;
    }

    public static final class Builder {
        private UUID id;
        private String name;
        private Event event;
        private List<Selection> selections = new ArrayList<>();

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

        public Builder withEvent(Event val) {
            event = val;
            return this;
        }

        public Builder withSelections(List<Selection> val) {
            selections = val;
            return this;
        }

        public Market build() {

            Market market = new Market(this);

            List<Selection> updatedSelections = new ArrayList<>();
            for(Selection selection : selections) {
                selection = Selection.newBuilder(selection).withMarket(market).build();
                updatedSelections.add(selection);
            }
            market.selections = updatedSelections;
            return market;
        }
    }
}
