package com.seoeun.ch2;

import java.io.Serializable;

/**
 * @author seoeun
 * @since ${VERSION} on 6/19/17
 */
public class Record implements Serializable {

    private long number;
    private long amount;

    public Record(long amount, long number) {
        this.amount = amount;
        this.number = number;
    }

    public Record(long amount) {
        this.amount = amount;
        this.number = 1;
    }

    public Record add(long amount) {
        this.amount += amount;
        this.number += 1;
        return this;
    }

    public Record add(Record other) {
        this.amount += other.amount;
        this.number += other.number;
        return this;
    }

    public String toString() {
        long avg = number == 0 ? 0L : (amount/number);
        return String.format("amount=%s, number=%s, avg=%s", amount, number, avg);
        //return "avg:" + amount / number;
    }

}
