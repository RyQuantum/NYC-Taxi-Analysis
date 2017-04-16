package topTips;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTipsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private TreeMap<Double, String> map = new TreeMap<Double, String>();

	public void map(LongWritable key, Text value, Context context) {
		String[] line = value.toString().split(",");
		if(line.length < 19) {
			return;
		}
		double tips = Double.parseDouble(line[16]);
		double total = Double.parseDouble(line[18]);
		
		map.put(tips/total, value.toString());
		if (map.size() > 10000) {
			map.remove(map.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (double key: map.keySet()) {
			context.write(NullWritable.get(), new Text(map.get(key)));
		}
	}
}