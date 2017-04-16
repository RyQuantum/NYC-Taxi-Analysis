package hourlyEfficiency;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner<K, V> extends Partitioner<K, V>{

	@Override
	public int getPartition(K hour, V arg1, int arg2) {
		// TODO Auto-generated method stub
		return Integer.parseInt(((Text)hour).toString());
	}

}
