package com.datinko.prototype.bigdata2.core.factories.random;

import org.joda.money.Money;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by neild on 05/09/2015.
 */
public class RandomStakeFactory {


    protected List<Money> stakes = new ArrayList<>();
    protected List<Money> lowValueStakes = new ArrayList<>();
    protected List<Money> highValueStakes = new ArrayList<>();
    protected List<Money> ultraHighValueStakes = new ArrayList<>();
    protected Random rand = new Random();

    public RandomStakeFactory() {

        lowValueStakes.add(Money.parse("GBP 1"));
        lowValueStakes.add(Money.parse("GBP 2"));
        lowValueStakes.add(Money.parse("GBP 5"));
        lowValueStakes.add(Money.parse("GBP 10"));
        lowValueStakes.add(Money.parse("GBP 15"));
        lowValueStakes.add(Money.parse("GBP 20"));
        lowValueStakes.add(Money.parse("GBP 25"));
        lowValueStakes.add(Money.parse("GBP 50"));
        lowValueStakes.add(Money.parse("GBP 100"));
        lowValueStakes.add(Money.parse("GBP 200"));
        lowValueStakes.add(Money.parse("GBP 250"));

        highValueStakes.add(Money.parse("GBP 500"));
        highValueStakes.add(Money.parse("GBP 1000"));
        highValueStakes.add(Money.parse("GBP 2000"));
        highValueStakes.add(Money.parse("GBP 3000"));
        highValueStakes.add(Money.parse("GBP 4000"));
        highValueStakes.add(Money.parse("GBP 5000"));

        ultraHighValueStakes.add(Money.parse("GBP 10000"));
        ultraHighValueStakes.add(Money.parse("GBP 15000"));
        ultraHighValueStakes.add(Money.parse("GBP 20000"));
        ultraHighValueStakes.add(Money.parse("GBP 25000"));
        ultraHighValueStakes.add(Money.parse("GBP 30000"));
        ultraHighValueStakes.add(Money.parse("GBP 40000"));
        ultraHighValueStakes.add(Money.parse("GBP 50000"));
        ultraHighValueStakes.add(Money.parse("GBP 100000"));
        ultraHighValueStakes.add(Money.parse("GBP 200000"));
        ultraHighValueStakes.add(Money.parse("GBP 250000"));
        ultraHighValueStakes.add(Money.parse("GBP 300000"));
        ultraHighValueStakes.add(Money.parse("GBP 400000"));
        ultraHighValueStakes.add(Money.parse("GBP 500000"));

        stakes.add(Money.parse("GBP 1"));
        stakes.add(Money.parse("GBP 2"));
        stakes.add(Money.parse("GBP 5"));
        stakes.add(Money.parse("GBP 10"));
        stakes.add(Money.parse("GBP 15"));
        stakes.add(Money.parse("GBP 20"));
        stakes.add(Money.parse("GBP 25"));
        stakes.add(Money.parse("GBP 50"));
        stakes.add(Money.parse("GBP 100"));
        stakes.add(Money.parse("GBP 200"));
        stakes.add(Money.parse("GBP 250"));
        stakes.add(Money.parse("GBP 500"));
        stakes.add(Money.parse("GBP 1000"));
        stakes.add(Money.parse("GBP 2000"));
        stakes.add(Money.parse("GBP 3000"));
        stakes.add(Money.parse("GBP 4000"));
        stakes.add(Money.parse("GBP 5000"));
        stakes.add(Money.parse("GBP 10000"));
        stakes.add(Money.parse("GBP 15000"));
        stakes.add(Money.parse("GBP 20000"));
        stakes.add(Money.parse("GBP 25000"));
        stakes.add(Money.parse("GBP 30000"));
        stakes.add(Money.parse("GBP 40000"));
        stakes.add(Money.parse("GBP 50000"));
        stakes.add(Money.parse("GBP 100000"));
        stakes.add(Money.parse("GBP 200000"));
        stakes.add(Money.parse("GBP 250000"));
        stakes.add(Money.parse("GBP 300000"));
        stakes.add(Money.parse("GBP 400000"));
        stakes.add(Money.parse("GBP 500000"));

    }

    public Money getRandomStake() {

        int index = rand.nextInt(stakes.size() - 1);
        return stakes.get(index);
    }

    public Money getRandomHighValueStake() {

        int index = rand.nextInt(highValueStakes.size() - 1);
        return highValueStakes.get(index);
    }

    public Money getRandomUltraHighValueStake() {

        int index = rand.nextInt(ultraHighValueStakes.size() - 1);
        return ultraHighValueStakes.get(index);
    }

    public Money getRandomLowValueStake() {

        int index = rand.nextInt(lowValueStakes.size() - 1);
        return lowValueStakes.get(index);
    }
}
