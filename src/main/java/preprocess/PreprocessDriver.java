package preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PreprocessDriver {

	public static void main(String[] args) throws Exception {
		for (int i = 1; i <= 12; i++) {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("Usage: PreprocessDriver <input(data + fare)> <outdir>");
				System.exit(1);
			}

			Job job = Job.getInstance(conf, "Preprocess");
			job.setJarByClass(PreprocessDriver.class);

			MultipleInputs.addInputPath(job, new Path(otherArgs[0] + "/data/trip_data_" + i + ".csv"), TextInputFormat.class, TripDataMapper.class);
			MultipleInputs.addInputPath(job, new Path(otherArgs[0] + "/fare/trip_fare_" + i + ".csv"), TextInputFormat.class, TripFareMapper.class);

			job.setReducerClass(PreprocessReducer.class);

			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/Month" + i));

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true);
		}

		System.exit(0);
	}

}
