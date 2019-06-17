import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

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

        PCollection<Integer> pc1 = p.apply(Create.of(1, 2, 3, 4, 5));
        PCollection<Integer> pc2 = p.apply(Create.of(6, 7, 8, 9, 10));

        PCollectionList<Integer> collections = PCollectionList.of(pc1).and(pc2);

        collections.apply(Flatten.<Integer>pCollections())
                .apply(MapElements.into(TypeDescriptors.strings()).via(String::valueOf))
                .apply(TextIO.write().to("src/main/java/org/apache/beam/examples/CoreTransforms/Flatten/output").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
