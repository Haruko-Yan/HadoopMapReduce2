package group.s3749857.Assignment2Task2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RelativeFrequencyMapper extends Mapper<LongWritable,Text,WordPair,IntWritable> {

	private static final Logger LOG = Logger.getLogger(RelativeFrequencyMapper.class);
	private Map<WordPair, Integer> map = new HashMap<WordPair, Integer>(); // Initialize a Map for achieving in-mapper combining
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Set log-level to debugging
		LOG.setLevel(Level.DEBUG);
				
		LOG.debug("The mapper task of Ziqing Yan, s3749857");
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		String token;
		
		String[] neighbor = new String[4]; // The 4 neighbors before the current word
		
		// Traverse the input words
		while (tokenizer.hasMoreElements()) {
			token = tokenizer.nextToken();
			for (int i = 0; i < neighbor.length; i++) {
				// The array neighbor contains null value when the current word is the first to fourth word
				if (neighbor[i] == null) break;
				// don't count the pair of same words
				else if (neighbor[i].equals(token)) continue;
				else {
					WordPair pair1 = new WordPair(token, neighbor[i]); // word pair of current word and the neighbor
					WordPair count1 = new WordPair(token, "*"); // For counting the word
					
					WordPair pair2 = new WordPair(neighbor[i], token); // word pair of neighbor and current word
					WordPair count2 = new WordPair(neighbor[i], "*"); // For counting the word
					
					// Obviously, the map must contain pair2 if it contains pair1
					if (map.containsKey(pair1) && map.containsKey(pair2)) {
						map.put(pair1, map.get(pair1) + 1);
						map.put(pair2, map.get(pair2) + 1);
					}
					// Otherwise, add new key-pair into map
					else {
						map.put(pair1, 1);
						map.put(pair2, 1);
					}
					if (map.containsKey(count1)) map.put(count1, map.get(count1) + 1);
					else map.put(count1, 1);
					if (map.containsKey(count2)) map.put(count2, map.get(count2) + 1);
					else map.put(count2, 1);
				}
			}
			
			// Replace the neighbors with new neighbors of the next word
			for (int i = neighbor.length - 1; i > 0; i--) {
				neighbor[i] = neighbor[i - 1];
			}
			neighbor[0] = token;
		}
		
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, WordPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// Traverse key-value pairs and transform them to Reducer
		for (Entry<WordPair, Integer> entry : map.entrySet()) {
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
		}
	}
}
