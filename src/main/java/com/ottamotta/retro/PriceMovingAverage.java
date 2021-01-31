package com.ottamotta.retro;

import com.ottamotta.retro.indicator.Indicator;
import com.ottamotta.retro.indicator.TickWithIndicators;
import com.ottamotta.retro.tick.Tick;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PriceMovingAverage {

    public interface MovingAverageOptions extends PipelineOptions {

        @Description("Path to input file")
        @Default.String("./bitstampUSD_1-min_data_2012-01-01_to_2020-12-31.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

    }

    public static class ExtractTicksFn extends DoFn<String, Tick> {

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<Tick> outputReceiver) {
            String[] columns = element.split(",");
            try {
                long timestamp = Long.parseLong(columns[0]);
                double open = Double.parseDouble(columns[1]);
                double high = Double.parseDouble(columns[2]);
                double low = Double.parseDouble(columns[3]);
                double close = Double.parseDouble(columns[4]);
                Tick tick = new Tick(timestamp, open, high, low, close);
                outputReceiver.outputWithTimestamp(tick, Instant.ofEpochSecond(timestamp));
            } catch (Exception ignored) {
            }
        }
    }

    public static class PriceMovingAverageTransform extends PTransform<PCollection<String>, PCollection<TickWithIndicators>> {

        @Override
        public PCollection<TickWithIndicators> expand(PCollection<String> input) {

            return input
                    .apply("ExtractTicks", ParDo.of(new ExtractTicksFn()))
                    .apply("ApplyWindows", Window.<Tick>into(SlidingWindows.of(Duration.standardMinutes(6))))
                    .apply("ExtractIndicators", new Indicator.ExtractIndicators());

        }
    }

}
