/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ottamotta.retro;

import com.ottamotta.retro.indicator.TickWithIndicators;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {

  public static void main(String[] args) {

    PriceMovingAverage.MovingAverageOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PriceMovingAverage.MovingAverageOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply("CalculateTicksWithAverageIndicator", new PriceMovingAverage.PriceMovingAverageTransform())
            .apply("FormatIndicatorsAsText", MapElements.via(new FormatAsTextFn<>()))
            .apply("WriteAverages", TextIO.write().to(options.getOutput()));
    pipeline.run().waitUntilFinish();
  }

  /**
   * A SimpleFunction that converts a Word and Count into a printable string.
   */
  public static class FormatAsTextFn<T> extends SimpleFunction<T, String> {
    @Override
    public String apply(T input) {
      return input.toString();
    }
  }
}
