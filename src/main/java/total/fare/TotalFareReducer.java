package total.fare;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalFareReducer extends
		Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
	
	private DoubleWritable outValue = new DoubleWritable();
	
	public void reduce(NullWritable key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
		}
		outValue.set(sum);
		context.write(NullWritable.get(), outValue);
	}
}