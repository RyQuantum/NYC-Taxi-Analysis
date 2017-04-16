package preprocess;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text outValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String data = null;
		String fare = null;
		for (Text value : values) {
			if (value.charAt(0) == 'd') {
				data = value.toString().substring(1, value.toString().length());
			} else {
				fare = value.toString().substring(1, value.toString().length());
			}
		}
		outValue.set(data + "," + fare);
		context.write(NullWritable.get(), outValue);

	}

}