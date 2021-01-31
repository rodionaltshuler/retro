package com.ottamotta.retro;

import com.ottamotta.retro.indicator.TickWithIndicators;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class EnrichTicksWithIndicatorsPipeline {

  public static void main(String[] args) {

    EnrichTicksWithIndicatorsPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(EnrichTicksWithIndicatorsPipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply("EnrichTicksWithIndicators", new TickWithIndicators.EnrichTicksWithIndicatorsTransform())
            .apply("FormatOutputAsText", MapElements.via(new FormatAsTextFn<>()))
            .apply("WriteTicksWithIndicators", TextIO.write().to(options.getOutput()));
    pipeline.run().waitUntilFinish();
  }

  public interface EnrichTicksWithIndicatorsPipelineOptions extends PipelineOptions {

      @Description("Path to input file")
      @Default.String("./bitstampUSD_1-min_data_2012-01-01_to_2020-12-31.csv")
      String getInputFile();

      void setInputFile(String value);

      @Description("Path of the file to write to")
      @Validation.Required
      String getOutput();

      void setOutput(String value);

  }

  public static class FormatAsTextFn<T> extends SimpleFunction<T, String> {
    @Override
    public String apply(T input) {
      return input.toString();
    }
  }
}
