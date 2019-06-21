import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class Main {

    // 単語数を数えるTransform
    static class ComputeWordCount extends PTransform<PCollection<String>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            return input.apply("split with half space", MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                    .via(element -> Arrays.asList(element.split(" "))))
                    .apply("compute array size", MapElements.into(TypeDescriptors.strings())
                            .via((List<String> element) -> String.valueOf(element.size())));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        List<String> inputs = Arrays.asList("There is no time like the present.", "Time is money.");

        p.apply("create inputs", Create.of(inputs))
                .apply("compute word count", new ComputeWordCount())
                .apply("write to text", TextIO.write().to("src/main/java/org/apache/beam/examples/CompositeTransforms/output"));

        p.run().waitUntilFinish();
    }
}
