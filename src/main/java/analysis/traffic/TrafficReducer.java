package analysis.traffic;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable outValue = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		int i = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
			i++;
//			context.write(key, value);
		}
		outValue.set(sum / i);
		context.write(key, outValue);
	}
}