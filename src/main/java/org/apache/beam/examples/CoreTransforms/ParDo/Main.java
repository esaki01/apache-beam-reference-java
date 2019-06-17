import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;

public class Main {
    static class ComputeWordLengthFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<String> out) {
            out.output(String.valueOf(word.length()));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        p.apply(Create.of(Arrays.asList("good morning.", "good afternoon.", "good evening.")))
                .apply(ParDo.of(new ComputeWordLengthFn()))
                .apply(TextIO.write().to("src/main/java/org/apache/beam/examples/CoreTransforms/ParDo/output").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
