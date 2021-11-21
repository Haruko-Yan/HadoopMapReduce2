package group.s3749857.Assignment2Task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelativeFrequencyPartitioner extends Partitioner<WordPair, IntWritable> {

	@Override
	public int getPartition(WordPair key, IntWritable value, int numPartitions) {
		// Word pairs with the same first word will be allocated to the same reducer
		return (key.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
