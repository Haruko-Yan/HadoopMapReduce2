package group.s3749857.Assignment2Task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CooccurrenceFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static final Logger LOG = Logger.getLogger(CooccurrenceFrequencyReducer.class);
	private IntWritable result = new IntWritable(); // the sum of the same type of word pairs
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, 
	InterruptedException {
		// Set log-level to debugging
		LOG.setLevel(Level.DEBUG);
		
		LOG.debug("The reducer task of Ziqing Yan, s3749857");
		
		// initialize the sum
		int sum = 0; 
		for (IntWritable v : value) {
			sum += v.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
