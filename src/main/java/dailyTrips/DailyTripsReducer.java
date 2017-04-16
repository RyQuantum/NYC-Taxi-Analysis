package dailyTrips;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyTripsReducer extends
Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable outValue = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		outValue.set(sum);
		context.write(key, outValue);
	}
}