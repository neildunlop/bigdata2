package com.datinko.prototype.bigdata2.core.firer;

import com.datinko.prototype.bigdata2.core.Bet;
import com.datinko.prototype.bigdata2.core.factories.BetFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A simple event firer that produces events.  Events can be replayed from a historical log.
 */
public class DemoFirer {

    private static List<Bet> bets = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {

        bets.add(BetFactory.getAmyBrownBetting10OnMiddlesbroughToWinFromLeedsMerrion());
        bets.add(BetFactory.getBobSmithBetting20OnMiddlesbroughToWinFromLeedsMerrion());
        bets.add(BetFactory.getEveWhitworthBetting5OnMiddlesbroughToWinFromLeedsMerrion());

        DemoFirer firer = new DemoFirer();
        firer.fireEvents();
    }

    public void fireEvents() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);  //means a limit of 100 concurrent runnables

        for(Bet bet : bets) {
            //create a runnable task that we can execute
            Runnable task = () -> {
                DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                System.out.println("Firing bet " + bet.getId() + " at "+ DateTime.now().toString(fmt));
            };

            DateTime d1 = DateTime.now();
            DateTime d2 = bet.getTimestamp();

            long diffInMillis = d2.getMillis() - d1.getMillis();

            if(diffInMillis<0) {
                diffInMillis = 0;
            }

            //Schedule the runnable task to run after a delay of three seconds
            ScheduledFuture<?> future = executor.schedule(task, diffInMillis, TimeUnit.MILLISECONDS);
        }
    }


}
