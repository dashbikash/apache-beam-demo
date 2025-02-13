package dashbikash.beamspark;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


public class BeamSparkApp 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String header="id,email,phone,country,age";
        String OUTPUT_DIR=System.getenv("HOME")+"/tmp";
        SparkPipelineOptions options=PipelineOptionsFactory.as(SparkPipelineOptions.class);
        options.setRunner(SparkRunner.class);
        options.setAppName("BeamSparkDemo");
        //options.setSparkMaster("local[2]");
        options.setSparkMaster("spark://localhost:7077");
        Pipeline p=Pipeline.create(options);
        PCollection<String> input= p.apply("ReadCsv", TextIO.read().from("src/main/resources/sample_dataset.csv"));
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
