import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Arrays;
import java.util.List;

public class Main {
    static class ToStringFn extends DoFn<KV<String, CoGbkResult>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, CoGbkResult> in, OutputReceiver<String> out) {
            out.output(String.valueOf(in));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline p = Pipeline.create(options);

        List<KV<String, String>> emailsList = Arrays.asList(
                KV.of("amy", "amy@example.com"),
                KV.of("carl", "carl@example.com"),
                KV.of("julia", "julia@example.com"),
                KV.of("carl", "carl@email.com"));

        List<KV<String, String>> phonesList = Arrays.asList(
                KV.of("amy", "111-222-3333"),
                KV.of("james", "222-333-4444"),
                KV.of("amy", "333-444-5555"),
                KV.of("carl", "444-555-6666"));

        PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
        PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));

        final TupleTag<String> emailsTag = new TupleTag<>();
        final TupleTag<String> phonesTag = new TupleTag<>();

        KeyedPCollectionTuple.of(emailsTag, emails).and(phonesTag, phones)
                .apply(CoGroupByKey.<String>create())
                .apply(ParDo.of(new ToStringFn()))
                .apply(TextIO.write().to("src/main/java/org/apache/beam/examples/CoreTransforms/CoGroupByKey/output").withSuffix(".txt"));

        p.run().waitUntilFinish();
    }
}
