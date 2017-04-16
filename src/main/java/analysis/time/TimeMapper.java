package analysis.time;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text outKey = new Text();
	private IntWritable outValue = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(",");
		String[] datetime = line[3].split(" ");
		String[] time = datetime[1].split(":");
		String hour = time[0];
		outKey.set(hour);
		
		context.write(outKey, outValue);
	}
}