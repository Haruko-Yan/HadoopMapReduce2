package group.s3749857.Assignment2Task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PAMMapper extends Mapper<LongWritable, Text, Medoid, DataPoint> {
	
	private List<Medoid> centers = new ArrayList<Medoid>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// Get the path of the file containing the medoids from the configuration of the job
		Configuration conf = context.getConfiguration();
		Path medoids = new Path(conf.get("medoid.path"));
		FileSystem fs = FileSystem.get(conf);

		// Read these medoids and store them to the ArrayList
		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, medoids, conf)) {
			Medoid key = new Medoid();
			IntWritable value = new IntWritable();
			while(reader.next(key, value)) {
				Medoid medoid = new Medoid(key);
				centers.add(medoid);
			}
		}
		
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Step 2 start
		String[] location = value.toString().split(" ");
		DataPoint dataPoint = new DataPoint(Double.parseDouble(location[0]), Double.parseDouble(location[1]));
		Medoid medoid = null;
		double minDistance = Double.MAX_VALUE;
		
		// Find the nearest medoid of the current data point
		for (Medoid center : centers) {
			double dist = EuclidianDistance.measureDistance(center, dataPoint);
			if (dist < minDistance || medoid == null) {
				minDistance = dist;
				medoid = center;
			}
		}
		context.write(medoid, dataPoint);
		// Step 2 end
	}

}
