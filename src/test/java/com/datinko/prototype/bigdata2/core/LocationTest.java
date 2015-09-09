package com.datinko.prototype.bigdata2.core;

import org.junit.Test;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

/**
 * Tests to confirm the Location class behaves as expected.
 */
public class LocationTest {

    @Test
    public void canCreateLocation() {

        UUID id = UUID.randomUUID();
        String address = "Merrion Centre";
        ChannelType channelType = ChannelType.RETAIL;

        Location testLocation = Location.newBuilder()
                .withId(id)
                .withAddress(address)
                .withChannel(channelType)
                .build();

        assertNotNull(testLocation);
        assertEquals(id, testLocation.getId());
        assertEquals(address, testLocation.getAddress());
        assertEquals(channelType, testLocation.getChannel());
    }

}
