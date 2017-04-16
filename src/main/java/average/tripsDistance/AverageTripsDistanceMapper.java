package average.tripsDistance;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageTripsDistanceMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {

	private DoubleWritable outValue = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");

		double distance = Double.parseDouble(line[7]);
		if(distance > 25) {
			return;
		}
		outValue.set(distance);
		context.write(NullWritable.get(), outValue);
	}
}