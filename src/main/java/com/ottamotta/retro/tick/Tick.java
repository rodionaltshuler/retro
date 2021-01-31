package com.ottamotta.retro.tick;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Tick implements Serializable {

    private long timestamp;
    private double open;
    private double high;
    private double low;
    private double close;

    public Tick() {
    }

    public Tick(long timestamp, double open, double high, double low, double close) {
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getOpen() {
        return open;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tick tick = (Tick) o;
        return timestamp == tick.timestamp && Double.compare(tick.open, open) == 0 && Double.compare(tick.high, high) == 0 && Double.compare(tick.low, low) == 0 && Double.compare(tick.close, close) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, open, high, low, close);
    }

    @Override
    public String toString() {
        return String.join(",",String.valueOf(timestamp), String.valueOf(open), String.valueOf(high),
                String.valueOf(low), String.valueOf(close));
    }
}
