package com.datinko.prototype.bigdata2.core;

import org.junit.Test;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * Tests to confirm the Customer class behaves as expected.
 */
public class CustomerTest {

    @Test
    public void canCreateCustomer() {

        UUID id = UUID.randomUUID();
        String name = "Bob Smith";

        Customer testCustomer = Customer.newBuilder()
                .withId(id)
                .withName(name)
                .build();

        assertNotNull(testCustomer);
        assertEquals(id, testCustomer.getId());
        assertEquals(name, testCustomer.getName());

    }

}
