package average.date.trips;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageTripsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text outKey = new Text();
	private IntWritable outValue = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(",");
		String[] datetime = line[3].split(" ");
		String[] date = datetime[0].split("-");
		String day = date[2];
		outKey.set(day);
		
		context.write(outKey, outValue);
	}
}