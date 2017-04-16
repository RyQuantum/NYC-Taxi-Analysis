package total.distance;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalDistanceMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {

	private DoubleWritable outValue = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");

		double distance = Double.parseDouble(line[7]);
		
		outValue.set(distance);
		context.write(NullWritable.get(), outValue);
	}
}