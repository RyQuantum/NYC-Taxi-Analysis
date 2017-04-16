package total.passengers;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalPassengersMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {

	private IntWritable outValue = new IntWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");

		int passengers = Integer.parseInt((line[5]));
		
		outValue.set(passengers);
		context.write(NullWritable.get(), outValue);
	}
}