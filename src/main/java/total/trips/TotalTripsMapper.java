package total.trips;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalTripsMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {

	private IntWritable outValue = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), outValue);
	}
}