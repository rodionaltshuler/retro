package com.ottamotta.retro.indicator;

import com.ottamotta.retro.tick.Tick;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Indicator implements Serializable {

    public static String SIMPLE_MOVING_AVERAGE = "SMA";

    private String name;
    private double value;

    public Indicator() {

    }

    public Indicator(String name, double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Indicator indicator = (Indicator) o;
        return Double.compare(indicator.value, value) == 0 && Objects.equals(name, indicator.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    public static class ExtractIndicators extends PTransform<PCollection<Tick>, PCollection<TickWithIndicators>> {

        @Override
        public PCollection<TickWithIndicators> expand(PCollection<Tick> ticks) {
            PCollection<TickWithIndicators> closingPrices  = ticks.apply(
                    Combine.globally(new TickWithIndicators.IndicatorCombineFn()).withoutDefaults());
            return closingPrices;
        }
    }
}
