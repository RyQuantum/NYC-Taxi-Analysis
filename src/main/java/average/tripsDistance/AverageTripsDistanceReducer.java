package average.tripsDistance;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageTripsDistanceReducer extends
		Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
	
	private DoubleWritable outValue = new DoubleWritable();
	
	public void reduce(NullWritable key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		for (DoubleWritable value : values) {
			outValue.set(value.get());
			context.write(NullWritable.get(), outValue);
		}
	}
}