package hadoop.movie.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// Import this class for NullOutputFormat
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class MovieReleaseCount {
    public static void main(String[] args) throws Exception {
        // Verify that two arguments (input and output paths) are passed
        if (args.length != 2) {
            System.err.println("Usage: MovieReleaseCount <input path> <output path>");
            System.exit(-1);
        }

        // Basic configuration for the MapReduce job
        Configuration conf = new Configuration();
        
        // Create a new instance of the job
        Job job = Job.getInstance(conf, "movie release year count");
        
        // Define the class that contains the program's entry point (main)
        job.setJarByClass(MovieReleaseCount.class);
        
        // Define the Mapper to use (CSVReleaseDateMapper)
        job.setMapperClass(CSVReleaseDateMapper.class);
        
        // The combiner (optional) also uses the Reducer in this case to optimize performance
        // If you wish to use a combiner, make sure it is compatible with the Reducer
        // job.setCombinerClass(IntSumReducer.class);
        
        // Define the Reducer to use (HBaseReducer)
        job.setReducerClass(HBaseReducer.class);
        
        // Define the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Input path of the CSV file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // If you do not wish to generate output in HDFS (since data is inserted into HBase),
        // you can use NullOutputFormat
        job.setOutputFormatClass(NullOutputFormat.class);

        // Launch the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
