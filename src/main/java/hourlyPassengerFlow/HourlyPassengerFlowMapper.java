package hourlyPassengerFlow;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class HourlyPassengerFlowMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");

		String[] datetime = line[3].split(" ");
		String[] time = datetime[1].split(":");
		String hour = time[0];
		outKey.set(hour);
		outValue.set(value.toString());
		context.write(outKey, outValue);
	}
}