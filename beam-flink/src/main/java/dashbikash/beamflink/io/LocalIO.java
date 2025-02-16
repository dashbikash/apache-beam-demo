package dashbikash.beamflink.io;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import dashbikash.beamflink.transform.HashingTransForm;

public class LocalIO {
	public static void RunPipeline() {

        String header="id,email,phone,country,age";
        String INPUT_FILE="src/main/resources/sample_dataset.csv";
        String OUTPUT_DIR=System.getenv("HOME")+"/tmp";
        
        FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
        options.setRunner(FlinkRunner.class);
        options.setParallelism(4);
        options.setFlinkMaster("[local]");
        
        Pipeline p=Pipeline.create(options);
        
        PCollection<String> input= p.apply("ReadCsv", TextIO.read().from(INPUT_FILE));
        PCollection<String> filteredLines=input
        		.apply(Filter.by((String line) -> !line.startsWith("id")))
        		.apply(Filter.by((String line) -> {
        			
        			return !line.split(",")[3].trim().equalsIgnoreCase("IN");
        		}));
        PCollection<String> hashedOutput=filteredLines.apply("Hashing",ParDo.of(new HashingTransForm()));
        hashedOutput.apply("WriteCsv",TextIO.write().to(OUTPUT_DIR+"/beam_output").withSuffix(".csv").withHeader(header).withoutSharding());
        p.run().waitUntilFinish();
	}
}
