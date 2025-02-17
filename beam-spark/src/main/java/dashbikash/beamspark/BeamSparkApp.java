package dashbikash.beamspark;

import java.util.Scanner;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import dashbikash.beamspark.io.AwsS3IO;

import org.apache.beam.sdk.io.aws.options.S3Options;

public class BeamSparkApp 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        
        AwsS3IO.RunPipeline();
        
        
        
    }
}
