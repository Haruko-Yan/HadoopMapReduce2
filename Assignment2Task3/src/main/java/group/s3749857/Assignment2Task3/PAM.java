package group.s3749857.Assignment2Task3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;





public class PAM extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {

		System.out.println("Started running Task3 job.");
		int k = Integer.parseInt(args[2]);
		Path medoidPath = new Path("clustering/medoid.seq");
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = new Configuration();
		conf.set("medoid.path", medoidPath.toString()); // add the medoid path to the configuration so that it can be shared to Mapper and Reducer
		Job job = Job.getInstance(conf);
		job.setJobName("K-Medoid Clustering");

		job.setMapperClass(PAMMapper.class);
		job.setReducerClass(PAMReducer.class);
		job.setJarByClass(PAM.class);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(medoidPath)) {
			fs.delete(medoidPath, true);
		}
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// Step 1 start
		initializeMedoids(conf, fs, inputPath, medoidPath, k); // initialize the medoids
		// Step 1 end
		
		job.setNumReduceTasks(1);
		
		// Set input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
		// Set the type of the output of key and value from mapper
        job.setMapOutputKeyClass(Medoid.class);
        job.setMapOutputValueClass(DataPoint.class);
        
        // Set the type of the output of key and value from reducer
        job.setOutputKeyClass(Medoid.class);
        job.setOutputValueClass(DataPoint.class);
		
        job.waitForCompletion(true);
        
        long counter = job.getCounters().findCounter(PAMReducer.Counter.CONVERGED).getValue();
        
        // Update the cluster until it is converged
        while (counter > 0) {
        	conf = new Configuration();
    		conf.set("medoid.path", medoidPath.toString());
    		job = Job.getInstance(conf);
    		job.setJobName("K-Medoid Clustering");

    		job.setMapperClass(PAMMapper.class);
    		job.setReducerClass(PAMReducer.class);
    		job.setJarByClass(PAM.class);
    		
    		fs = FileSystem.get(conf);
    		fs.delete(outputPath, true);
    		
    		FileInputFormat.setInputPaths(job, inputPath);
    		FileOutputFormat.setOutputPath(job, outputPath);
    		
    		job.setNumReduceTasks(1);
    		
    		// Set input and output format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
    		// Set the type of the output of key and value from mapper
            job.setMapOutputKeyClass(Medoid.class);
            job.setMapOutputValueClass(DataPoint.class);
            
            // Set the type of the output of key and value from reducer
            job.setOutputKeyClass(Medoid.class);
            job.setOutputValueClass(DataPoint.class);
    		
            job.waitForCompletion(true);
            counter = job.getCounters().findCounter(PAMReducer.Counter.CONVERGED).getValue();
        }
		return 0;
	}
	
	private void initializeMedoids(Configuration conf, FileSystem fs, Path in, Path out, int k) throws IOException {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(in))); // Read the file containing data points
		IntWritable value = new IntWritable(0);
		int count = 1;
		
		try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, out, Medoid.class, IntWritable.class)) {
			for (int i = 0; i < k * 100; i++) {
				String s = br.readLine();
				// Take the coordinates of the current row as the medoid every 100 rows
				if (count == 100) {
					String[] location = s.split(" ");
					// Initialize the cost of the medoid with the max value of Double
					Medoid medoid = new Medoid(
							Double.parseDouble(location[0]), Double.parseDouble(location[1]), Double.MAX_VALUE);
					writer.append(medoid, value);
					count = 0;
				}
				count++;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new PAM(), args));
	}

}
