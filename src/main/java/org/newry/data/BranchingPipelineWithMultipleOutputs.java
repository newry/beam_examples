package org.newry.data;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Arrays;
import java.util.List;

public class BranchingPipelineWithMultipleOutputs {
  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
    PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from("words.txt"));

    PCollection<String> allWords =
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
    final TupleTag<String> startsWithATag = new TupleTag<String>() {};
    final TupleTag<String> startsWithBTag = new TupleTag<String>() {};

    PCollectionTuple words =
        allWords.apply(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        if (c.element().toUpperCase().startsWith("A")) {
                          c.output(startsWithATag, c.element());
                        } else if (c.element().toUpperCase().startsWith("B")) {
                          c.output(startsWithBTag, c.element());
                        }
                      }
                    })
                .withOutputTags(startsWithATag, TupleTagList.of(startsWithBTag)));

    words.get(startsWithATag).apply("WriteAFile", TextIO.write().to("result_a.txt"));
    words.get(startsWithBTag).apply("WriteAFile", TextIO.write().to("result_b.txt"));

    pipeline.run().waitUntilFinish();
  }
}
