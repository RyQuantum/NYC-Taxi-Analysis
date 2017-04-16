package total.passengers;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalPassengersReducer extends
		Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
	
	private IntWritable outValue = new IntWritable();
	
	public void reduce(NullWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		outValue.set(sum);
		context.write(NullWritable.get(), outValue);
	}
}