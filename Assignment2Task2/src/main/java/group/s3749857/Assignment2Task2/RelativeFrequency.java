package group.s3749857.Assignment2Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RelativeFrequency extends Configured implements Tool {

private static final Logger LOG = Logger.getLogger(RelativeFrequency.class);
	
	public int run(String[] args) throws Exception {
    	System.out.println("Started running Task2 job.");
    	// Get the configuration that is set by command line
    	Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Task2");
        
        job.setJarByClass(RelativeFrequency.class);
        
  		// Set input and output path entered by command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
  		
        // Set log-level to information
        LOG.setLevel(Level.INFO);
        
    	// Log all the arguments passed to the application
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);
        
        // Set the tasks
        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setPartitionerClass(RelativeFrequencyPartitioner.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        
        // Set the type of the output of key and value from mapper
        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        
  		// Set the number of reduce tasks
        job.setNumReduceTasks(3);
        
        // Set input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the type of the output of key and value from reducer
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(FloatWritable.class);
  		
        System.exit(job.waitForCompletion(true)? 0 : 1);
        return 0;
    }
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RelativeFrequency(), args));
	}

}
