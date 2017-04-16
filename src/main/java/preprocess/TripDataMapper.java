package preprocess;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TripDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if (key.get() == 0) {
			return;
		}
		String[] line = value.toString().split(",");
		outKey.set(line[0] +"," + line[1] +"," + line[5]);
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(int i = 2; i < line.length; i++) {
			if(first) {
				first = false;
			} else {
				sb.append(",");
			}
			sb.append(line[i]);
		}
		outValue.set("d" + sb.toString());
		context.write(outKey, outValue);
	}
}