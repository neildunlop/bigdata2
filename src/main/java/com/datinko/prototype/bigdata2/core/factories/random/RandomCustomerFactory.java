package com.datinko.prototype.bigdata2.core.factories.random;


import com.datinko.prototype.bigdata2.core.Customer;
import com.datinko.prototype.bigdata2.core.factories.CustomerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by neild on 05/09/2015.
 */
public class RandomCustomerFactory {

    protected List<Customer> customers = new ArrayList<>();
    protected Random rand = new Random();

    public RandomCustomerFactory() {
        this.customers.add(CustomerFactory.getAmyBrown());
        this.customers.add(CustomerFactory.getBobSmith());
        this.customers.add(CustomerFactory.getColinBladen());
        this.customers.add(CustomerFactory.getEmmaGreen());
        this.customers.add(CustomerFactory.getEricSmith());
        this.customers.add(CustomerFactory.getEveWhitworth());
        this.customers.add(CustomerFactory.getPeteRose());
        this.customers.add(CustomerFactory.getSarahWhite());
        this.customers.add(CustomerFactory.getSteveJones());
        this.customers.add(CustomerFactory.getTonyGold());
    }

    public Customer getKnownRandomCustomer() {

        int index = rand.nextInt(customers.size() - 1);
        return customers.get(index);
    }

    public Customer getAnonymousCustomer() {

        return CustomerFactory.getAnonymous();
    }
}
