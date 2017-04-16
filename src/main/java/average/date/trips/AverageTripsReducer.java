package average.date.trips;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageTripsReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable outValue = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		int day = Integer.parseInt(key.toString());
		if (day < 29) {
			outValue.set((int) (sum / 12.0));
		} else if (day < 31) {
			outValue.set((int) (sum / 11.0));
		} else {
			outValue.set((int) (sum / 7.0));
		}
		context.write(key, outValue);
	}
}