import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class Main {
    static class SplitTextsFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(@Element String in, OutputReceiver<KV<String, Integer>> out) {
            String[] words = in.split(",");
            out.output(KV.of(words[0], Integer.parseInt(words[1])));
        }
    }

    static class ToStringFn extends DoFn<KV<String, Iterable<Integer>>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Integer>> in, OutputReceiver<String> out) {
            out.output(String.valueOf(in));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from("src/main/java/org/apache/beam/examples/CoreTransforms/GroupByKey/input.txt"))
                .apply(ParDo.of(new SplitTextsFn()))
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new ToStringFn()))
                .apply(TextIO.write().to("src/main/java/org/apache/beam/examples/CoreTransforms/GroupByKey/output").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
