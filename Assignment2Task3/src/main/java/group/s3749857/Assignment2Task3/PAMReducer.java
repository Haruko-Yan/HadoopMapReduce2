package group.s3749857.Assignment2Task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class PAMReducer extends Reducer<Medoid, DataPoint, Medoid, DataPoint>{
	
	private List<Medoid> medoids = new ArrayList<Medoid>(); // for storing the updated medoid
	
	/**
	 * A flag indicates if the clustering converges.
	 */
	public static enum Counter {
		CONVERGED
	}
	
	@Override
	protected void reduce(Medoid key, Iterable<DataPoint> value, Context context) throws IOException, InterruptedException {
		
		List<DataPoint> pointList = new ArrayList<>(); // for storing the data points in the same cluster
		
		for (DataPoint dataPoint : value) {
			pointList.add(new DataPoint(dataPoint));
		}
		
		// Step 3 start
		Medoid medoid = null; // the medoid with the minimum average cost
		double minCost = Double.MAX_VALUE; // the minimum average cost of the data point becoming the medoid 
		
		// Traverse all data point in this cluster and calculate the average cost when they become the medoid of the cluster
		// Finally, we will get the data point with the minimum average cost
		for (DataPoint asMedoid : pointList) {
			double avgCost = 0; // the average cost when regarding the current data point as the medoid
			// Calculate the average cost of the current data point as the medoid
			for (DataPoint dataPoint : pointList) {
				avgCost += EuclidianDistance.measureDistance(asMedoid, dataPoint) / pointList.size();
			}
			if (avgCost < minCost || medoid == null) {
				minCost = avgCost;
				medoid = new Medoid(asMedoid, minCost);
			}
		}
		// The data points contains the original medoid, so if the original medoid is the best medoid,
		// the result medoid with the minimum cost will be the original medoid. Therefore, we can output
		// the result medoid as the updated medoid
		medoids.add(medoid);
		for (DataPoint dataPoint : pointList) {
			context.write(medoid, dataPoint);
		}
		// Check if it is converged. If all medoids are not replaced, the counter would be 0
		if (minCost < key.getCost()) {
			context.getCounter(Counter.CONVERGED).increment(1);
		}
		// Step 3 end
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		IntWritable value = new IntWritable(0);
		Configuration conf = context.getConfiguration();
		Path medoidPath = new Path(conf.get("medoid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(medoidPath, true);
		
		// Update the medoids
		try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, medoidPath, Medoid.class, IntWritable.class)) {
			for (Medoid medoid : medoids) {
				writer.append(medoid, value);
			}
		} 
	}

}
