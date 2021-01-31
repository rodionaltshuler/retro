package com.ottamotta.retro.indicator;

import com.ottamotta.retro.tick.Tick;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class TickWithIndicators implements Serializable {

    private Long timestamp;
    private int period;
    private Map<String, Indicator> indicators;

    public TickWithIndicators() {
    }

    public TickWithIndicators(Long timestamp, Map<String, Indicator> indicators, int period) {
        this.timestamp = timestamp;
        this.indicators = indicators;
        this.period = period;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Indicator> getIndicators() {
        return indicators;
    }

    public void setIndicators(Map<String,Indicator> indicators) {
        this.indicators = indicators;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TickWithIndicators that = (TickWithIndicators) o;
        return period == that.period && Objects.equals(timestamp, that.timestamp) && Objects.equals(indicators, that.indicators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, period, indicators);
    }

    @Override
    public String toString() {
        return String.join(",",
                String.valueOf(timestamp),
                String.valueOf(period),
                String.valueOf(indicators.get(Indicator.SIMPLE_MOVING_AVERAGE).getValue()));
    }

    public static class IndicatorCombineFn extends Combine.CombineFn<Tick, IndicatorCombineFn.Accum, TickWithIndicators> {

        @DefaultSchema(JavaBeanSchema.class)
        public static class Accum implements Serializable {
            long timestamp;
            double sum = 0;
            int count = 0;

            public long getTimestamp() {
                return timestamp;
            }

            public void setTimestamp(long timestamp) {
                this.timestamp = timestamp;
            }

            public double getSum() {
                return sum;
            }

            public void setSum(double sum) {
                this.sum = sum;
            }

            public int getCount() {
                return count;
            }

            public void setCount(int count) {
                this.count = count;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Accum accum = (Accum) o;
                return timestamp == accum.timestamp && Double.compare(accum.sum, sum) == 0 && count == accum.count;
            }

            @Override
            public int hashCode() {
                return Objects.hash(timestamp, sum, count);
            }
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum mutableAccumulator, Tick input) {
            mutableAccumulator.timestamp = Math.max(mutableAccumulator.timestamp, input.getTimestamp());
            mutableAccumulator.sum += input.getClose();
            mutableAccumulator.count++;
            return mutableAccumulator;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();
            for (Accum accum: accumulators) {
                merged.sum += accum.sum;
                merged.count += accum.count;
                merged.timestamp = Math.max(merged.timestamp, accum.timestamp);
            }
            return merged;
        }

        @Override
        public TickWithIndicators extractOutput(Accum accumulator) {
            Indicator indicator = new Indicator(Indicator.SIMPLE_MOVING_AVERAGE, accumulator.sum / accumulator.count);
            Map<String, Indicator> indicators = new HashMap<>();
            indicators.put(indicator.getName(), indicator);
            return new TickWithIndicators(accumulator.timestamp, indicators, accumulator.count);
        }


    }

    public static class EnrichTicksWithIndicatorsTransform extends PTransform<PCollection<String>, PCollection<TickWithIndicators>> {

        @Override
        public PCollection<TickWithIndicators> expand(PCollection<String> input) {

            return input
                    .apply("ExtractTicks", ParDo.of(new Tick.ExtractTicksFn()))
                    .apply("ApplyWindows", Window.<Tick>into(SlidingWindows.of(Duration.standardMinutes(6))))
                    .apply("ExtractIndicators", new Indicator.ExtractIndicators());

        }
    }
}
