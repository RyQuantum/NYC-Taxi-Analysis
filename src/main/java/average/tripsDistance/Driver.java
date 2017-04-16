package average.tripsDistance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TotalDistance");
		
		job.setJarByClass(Driver.class);
	    job.setMapperClass(AverageTripsDistanceMapper.class);
	    job.setReducerClass(AverageTripsDistanceReducer.class);
	    job.setCombinerClass(AverageTripsDistanceReducer.class);
	    
	    job.setNumReduceTasks(1);
	                
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);
	}

}
