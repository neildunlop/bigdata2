package com.datinko.prototype.bigdata2.core.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.joda.money.Money;

import java.io.IOException;

/**
 * Created by Neil on 05/09/2015.
 */
public class MoneyDeserializer extends StdDeserializer<Money> {
    public MoneyDeserializer() {
        super(Money.class);
    }

    @Override
    public Money deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        return Money.parse(jp.readValueAs(String.class));
    }
}
