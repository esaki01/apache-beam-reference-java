import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class Main {
    static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            int sum = 0;
            for (int item : input) {
                sum += item;
            }
            return sum;
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        p.apply(Create.of(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
                .apply(Combine.globally(new SumInts()))
                .apply(MapElements.into(TypeDescriptors.strings()).via(String::valueOf))
                .apply(TextIO.write().to("src/main/java/org/apache/beam/examples/CoreTransforms/Combine/output").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
