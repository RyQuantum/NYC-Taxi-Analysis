package hourlyTraffic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourlyTrafficDriver {

	public static void main(String[] args) throws Exception {
		
		Job job = Job.getInstance(new Configuration(), "HourlyTraffic");
	    
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setJarByClass(HourlyTrafficDriver.class);
	    job.setMapperClass(HourlyTrafficMapper.class);
	    job.setReducerClass(HourlyTrafficReducer.class);
	    job.setPartitionerClass(MyPartitioner.class);
	    
	    job.setNumReduceTasks(24);
	                
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
	        
	}