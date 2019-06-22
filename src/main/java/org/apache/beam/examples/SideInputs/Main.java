import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        List<String> inputs = Arrays.asList("good morning.", "good afternoon.", "good evening.");

        // 主入力
        PCollection<Integer> wordLengths = p.apply("create inputs", Create.of(inputs))
                .apply("compute word length", MapElements.into(TypeDescriptors.integers()).via(String::length));

        // 副入力
        PCollectionView<Double> meanWordLength = wordLengths
                .apply("compute mean word length", Mean.<Integer>globally().asSingletonView());

        // 平均以上の文字数を持つ文字列をフィルタリングする
        PCollection<String> aboveMeanWordLength = wordLengths.apply("filter above mean length", ParDo
                .of(new DoFn<Integer, String>() {
                    @ProcessElement
                    public void processElement(@Element Integer wordLength, OutputReceiver<String> out, ProcessContext c) {
                        if (wordLength >= c.sideInput(meanWordLength)) {
                            out.output(String.valueOf(wordLength));
                        }
                    }
                }).withSideInputs(meanWordLength)
        );

        aboveMeanWordLength.apply("write to text", TextIO.write().to("src/main/java/org/apache/beam/examples/SideInputs/output"));

        p.run().waitUntilFinish();
    }
}
