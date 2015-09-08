package com.datinko.prototype.bigdata2.core;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.UUID;

/**
 * Simple representation of a customer.
 */
@JsonDeserialize(builder=Customer.Builder.class)
public class Customer {

    protected final UUID id;
    protected final String name;

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    private Customer(Builder builder) {
        id = builder.id;
        name = builder.name;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Customer copy) {
        Builder builder = new Builder();
        builder.id = copy.id;
        builder.name = copy.name;
        return builder;
    }


    public static final class Builder {

        private UUID id;
        private String name;

        private Builder() {
        }

        public Builder withId(UUID id) {
            this.id = id;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Customer build() {
            return new Customer(this);
        }
    }
}
