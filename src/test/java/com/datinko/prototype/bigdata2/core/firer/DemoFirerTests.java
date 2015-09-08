package com.datinko.prototype.bigdata2.core.firer;

import org.junit.Test;

/**
 * Created by Neil on 26/08/2015.
 */
public class DemoFirerTests {

    @Test
    public void canFireEvents() throws InterruptedException {

        //yes I know this is a sucky test
        DemoFirer firer = new DemoFirer();
        firer.fireEvents();
    }

}
