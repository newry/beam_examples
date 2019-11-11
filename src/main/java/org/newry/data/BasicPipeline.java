package org.newry.data;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class BasicPipeline {

  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

    PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from("words.txt"));

    PCollection<String> words =
        lines
            .apply(
                "ReadWords",
                ParDo.of(
                    new DoFn<String, List<String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(Arrays.asList(StringUtils.split(c.element())));
                      }
                    }))
            .apply(
                ParDo.of(
                    new DoFn<List<String>, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.element().stream().map(it -> it.replaceAll("\\W", "")).forEach(c::output);
                      }
                    }));
    PCollection<KV<String, Long>> wordKV = words.apply("WordCount", Count.perElement());

    wordKV
        .apply(
            "ToString",
            ParDo.of(
                new DoFn<KV<String, Long>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().toString());
                  }
                }))
        .apply("WriteMyFile", TextIO.write().to("result.txt"));
    // run
    pipeline.run().waitUntilFinish();
  }
}
