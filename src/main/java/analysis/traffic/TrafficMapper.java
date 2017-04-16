package analysis.traffic;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text outKey = new Text();
	private DoubleWritable outValue = new DoubleWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
		int tripTimeInSec = Integer.parseInt(line[6]);
		if(tripTimeInSec == 0) {
			return;
		}
		double distance = Double.parseDouble(line[7]);
		double fare = Double.parseDouble(line[13]);
		double slowMinute = (fare - 2.5 - ((int)(distance / 0.2)) * 0.5) / 0.5;
		double slowSec = (int) slowMinute * 60;
		// if(slowSec > tripTimeInSec) {
		// slowSec = tripTimeInSec;
		// }
		double traffic = slowSec / tripTimeInSec;
		if(traffic > 1 || traffic < 0) {
			return;
		}
		outValue.set(traffic);

		String[] datetime = line[3].split(" ");
		String[] time = datetime[1].split(":");
		String hour = time[0];
		outKey.set(hour);

		context.write(outKey, outValue);
	}
}