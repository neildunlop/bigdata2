package com.datinko.prototype.bigdata2.core.factories.random;


import com.datinko.prototype.bigdata2.core.Selection;
import com.datinko.prototype.bigdata2.core.factories.SelectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by neild on 05/09/2015.
 */
public class RandomSelectionFactory {

    protected List<Selection> selections = new ArrayList<>();
    protected Random rand = new Random();

    public RandomSelectionFactory() {

        this.selections.add(SelectionFactory.getBristolToWin());
        this.selections.add(SelectionFactory.getDerbyToWin());
        this.selections.add(SelectionFactory.getBristolDerbyToDraw());

        this.selections.add(SelectionFactory.getMiddlesbroughToWin());
        this.selections.add(SelectionFactory.getHullToWin());
        this.selections.add(SelectionFactory.getMiddlesbroughHullToDraw());

        this.selections.add(SelectionFactory.getLeedsToWin());
        this.selections.add(SelectionFactory.getBlackburnToWin());
        this.selections.add(SelectionFactory.getLeedsBlackburnToDraw());

        this.selections.add(SelectionFactory.getCharltonToWin());
        this.selections.add(SelectionFactory.getBurnleyToWin());
        this.selections.add(SelectionFactory.getCharltonBurnleyToDraw());

        this.selections.add(SelectionFactory.getRotherhamToWin());
        this.selections.add(SelectionFactory.getBoltonToWin());
        this.selections.add(SelectionFactory.getRotherhamBoltonToDraw());

        this.selections.add(SelectionFactory.getWolvesToWin());
        this.selections.add(SelectionFactory.getReadingToWin());
        this.selections.add(SelectionFactory.getWolvesReadingToDraw());
    }

    public Selection getRandomSelection() {

        int index = rand.nextInt(selections.size() - 1);
        return selections.get(index);
    }
}
