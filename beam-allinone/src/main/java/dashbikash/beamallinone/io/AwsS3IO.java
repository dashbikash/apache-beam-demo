package dashbikash.beamallinone.io;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import dashbikash.beamallinone.transform.HashingTransForm;

public class AwsS3IO {
	public static void RunPipeline() {

        String header="id,email,phone,country,age";
        
        String accessKey = System.getenv("AWS_ACCESS_KEY_INPUT");
        String secretKey = System.getenv("AWS_SECRET_KEY_INPUT");
        
        
        String INPUT_FILE="s3://test-s3-input-lr/demo/sample_dataset.csv";
        String OUTPUT_DIR="s3://test-s3-input-lr/demo";
        //String OUTPUT_DIR="/tmp";

        FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(4);
        options.setFlinkMaster("[local]");
        
        // Set AWS credentials
        options.as(S3Options.class).setAwsRegion("eu-central-1");
        options.as(S3Options.class).setAwsCredentialsProvider(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(accessKey, secretKey)
        ));
        
        Pipeline p=Pipeline.create(options);
        PCollection<String> input= p.apply("ReadCsv", TextIO.read().from(INPUT_FILE));
        PCollection<String> filteredLines=input
        		.apply(Filter.by((String line) -> !line.startsWith("id")))
        		.apply(Filter.by((String line) -> {
        			
        			return !line.split(",")[3].trim().equalsIgnoreCase("IN");
        		}));
        PCollection<String> hashedOutput=filteredLines.apply("Hashing",ParDo.of(new HashingTransForm()));
        
        hashedOutput.apply("WriteCsv",TextIO.write().to(OUTPUT_DIR+"/beam_output_flink").withSuffix(".csv").withHeader(header).withoutSharding());
        p.run().waitUntilFinish();
	}
}
