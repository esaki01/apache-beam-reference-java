import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        List<String> inputs = Arrays.asList("good", "normal", "bad");

        TupleTag<String> even = new TupleTag<String>() {};
        TupleTag<String> odd = new TupleTag<String>() {};

        PCollection<Integer> wordLength = p.apply("create inputs", Create.of(inputs))
                .apply("compute word length", MapElements.into(TypeDescriptors.integers()).via(String::length));

        // 偶数か奇数かを判断する
        PCollectionTuple outputs = wordLength.apply("judge even or odd", ParDo.of(new DoFn<Integer, String>() {
            @ProcessElement
            public void processElement(@Element Integer wordLength, MultiOutputReceiver out) {
                if (wordLength % 2 == 0) {
                    out.get(even).output(String.valueOf(wordLength));
                } else {
                    out.get(odd).output(String.valueOf(wordLength));
                }
            }
        }).withOutputTags(even, TupleTagList.of(odd)));

        // 主出力
        outputs.get(even).apply("write to even.txt", TextIO.write().to("src/main/java/org/apache/beam/examples/AdditionalOutputs/even").withSuffix(".txt"));

        // 追加出力
        outputs.get(odd).apply("write to odd.txt", TextIO.write().to("src/main/java/org/apache/beam/examples/AdditionalOutputs/odd").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
