package com.datinko.prototype.bigdata2.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Simple representation of what the customer bet on.
 */
@JsonDeserialize(builder=Selection.Builder.class)
public class Selection {

    protected final UUID id;
    protected final String selectionValue;
    protected final String price;
    protected final Market market;

    //@JsonProperty("bets")
    @JsonIgnore
    protected List<Bet> bets;

    public UUID getId() {
        return id;
    }

    public String getSelectionValue() {
        return selectionValue;
    }

    public String getPrice() {
        return price;
    }

    public Market getMarket() {
        return market;
    }

    @JsonIgnore
    public List<Bet> getBets() {
        return Collections.unmodifiableList(bets);
    }

    private Selection(Builder builder) {
        id = builder.id;
        selectionValue = builder.selectionValue;
        price = builder.price;
        market = builder.market;
        bets = builder.bets;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Selection copy) {
        Builder builder = new Builder();
        builder.id = copy.id;
        builder.selectionValue = copy.selectionValue;
        builder.price = copy.price;
        builder.market = copy.market;
        builder.bets = copy.bets;
        return builder;
    }


    public static final class Builder {
        private UUID id;
        private String selectionValue;
        private String price;
        private Market market;
        private List<Bet> bets = new ArrayList<>();

        private Builder() {
        }

        public Builder withId(UUID val) {
            id = val;
            return this;
        }

        public Builder withSelectionValue(String val) {
            selectionValue = val;
            return this;
        }

        public Builder withPrice(String val) {
            price = val;
            return this;
        }

        public Builder withMarket(Market val) {
            market = val;
            return this;
        }

        public Builder withBets(List<Bet> val) {
            bets = val;
            return this;
        }

        public Selection build() {

            Selection selection = new Selection(this);

            List<Bet> updatedBets = new ArrayList<>();
            for(Bet bet : bets) {
                bet = Bet.newBuilder(bet).withSelection(selection).build();
                updatedBets.add(bet);
            }
            selection.bets = updatedBets;
            return selection;
        }
    }
}
