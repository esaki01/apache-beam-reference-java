import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Main {
    // カスタムオプション
    public interface MyOptions extends PipelineOptions {
        @Description("Input for the pipeline")
        @Default.String("gs://my-bucket/input")
        String getInput();
        void setInput(String input);

        @Description("Output for the pipeline")
        @Default.String("gs://my-bucket/input")
        String getOutput();
        void setOutput(String output);
    }

    // 文字数を求めるDoFnのサブクラス. 詳細は次の記事を参照.
    static class ComputeWordLengthFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<String> out) {
            out.output(String.valueOf(word.length()));
        }
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply("ReadFromText", TextIO.read().from(options.getInput())) // I/O Transformを適用してオプションで指定したパスにデータを読み込む
                .apply(ParDo.of(new ComputeWordLengthFn())) // ParDo Transformを適用（詳細は次の記事を参照）
                .apply(TextIO.write().to(options.getOutput())); // I/O Transformを適用してオプションで指定したパスにデータを書き込む

        p.run().waitUntilFinish();
    }
}
