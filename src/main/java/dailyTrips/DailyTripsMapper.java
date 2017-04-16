package dailyTrips;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyTripsMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Text outKey = new Text();
	private IntWritable outValue = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
		String[] datetime = line[3].split(" ");
		String[] date = datetime[0].split("-");
		String month = date[1];
		String day = date[2];
		outKey.set(month + "-" + day);

		context.write(outKey, outValue);
	}
}