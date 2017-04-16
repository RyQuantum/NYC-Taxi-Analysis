package total.fare;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalFareMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {

	private DoubleWritable outValue = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
		if(line.length < 19) {
			return;
		}
		double fare = Double.parseDouble(line[18]);
		
		outValue.set(fare);
		context.write(NullWritable.get(), outValue);
	}
}