package group.s3749857.Assignment2Task2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RelativeFrequencyReducer extends Reducer<WordPair, IntWritable, WordPair, FloatWritable> {

	private static final Logger LOG = Logger.getLogger(RelativeFrequencyReducer.class);
	private int count; // the sum of the word pair whose second word is '*'
	
	@Override
	protected void reduce(WordPair key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		
		// Set log-level to debugging
		LOG.setLevel(Level.DEBUG);
		
		LOG.debug("The reducer task of Ziqing Yan, s3749857");
		
		// the word pair whose second word is '*' is the first in order
		// Firstly, accumulate the total count of the word pair whose second word is '*'
		if (key.getNeighbor().toString().equals("*")) {
			count = 0;
			for (IntWritable v : value) {
				count += v.get();
			}
		}
		// Then, use the 'count' to calculate the relative frequency
		else {
			int sum = 0;
			for (IntWritable v : value) {
				sum += v.get();
			}
			context.write(key, new FloatWritable((float) sum / count));
		}
	}
}
