package dashbikash.beamallinone.io;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;

public class OptionFactory {
		private static final String RUNNER = System.getenv("RUNNER") != null ? System.getenv("RUNNER") : "Direct";
		
		
		public static PipelineOptions createOptions() {
			
			if (RUNNER.equalsIgnoreCase("Flink")) {
				FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
		        options.setRunner(FlinkRunner.class);
		        options.setParallelism(4);
		        options.setFlinkMaster("[local]");
			}
			
			if (RUNNER.equalsIgnoreCase("Spark")) {
				SparkPipelineOptions options=PipelineOptionsFactory.as(SparkPipelineOptions.class);
		        options.setRunner(SparkRunner.class);
		        options.setAppName("BeamSparkDemo");
		        options.setSparkMaster("local[2]");
		        return options;
				
			}
			
			return PipelineOptionsFactory.create();
	}

}
