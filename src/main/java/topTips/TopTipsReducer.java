package topTips;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTipsReducer extends Reducer<NullWritable, Text, NullWritable, Text>{
	
	private TreeMap<Double, String> map = new TreeMap<Double, String>();
	
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for(Text value: values ) {
			String[] line = value.toString().split(",");
			double tips = Double.parseDouble(line[16]);
			double total = Double.parseDouble(line[18]);
			map.put(tips/total, value.toString());
			if (map.size() > 10000) {
				map.remove(map.firstKey());
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (double key: map.descendingKeySet()) {
			context.write(NullWritable.get(), new Text(key + ","+ map.get(key).toString()));
		}
	}
}
