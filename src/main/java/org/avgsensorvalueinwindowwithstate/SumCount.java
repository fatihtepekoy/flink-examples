package org.avgsensorvalueinwindowwithstate;

public class SumCount {
    private final double sum;
    private final int count;

    public SumCount(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }
}
