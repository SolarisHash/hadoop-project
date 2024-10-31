package hadoop.movie.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieReleaseCount {
    public static void main(String[] args) throws Exception {
        // Check that two arguments (input and output path) are passed
        if (args.length != 2) {
            System.err.println("Usage: MovieReleaseCount <input path> <output path>");
            System.exit(-1);
        }

        // Basic MapReduce job configuration
        Configuration conf = new Configuration();
        
        // Create a new job instance
        Job job = Job.getInstance(conf, "movie release year count");
        
        // Define the class that contains the program's entry point (main)
        job.setJarByClass(MovieReleaseCount.class);
        
        // Define the Mapper to use (CSVReleaseDateMapper)
        job.setMapperClass(CSVReleaseDateMapper.class);
        
        // The combiner (optional) also uses the Reducer in this case to optimize performance
        job.setCombinerClass(IntSumReducer.class);
        
        // Define the Reducer to use (IntSumReducer)
        job.setReducerClass(IntSumReducer.class);
        
        // Define the output key and value classes
        job.setOutputKeyClass(Text.class);          // The output key is of type Text (the release year)
        job.setOutputValueClass(IntWritable.class); // The output value is of type IntWritable (number of movies)
        
        // Input path of the CSV file (args[0] is the first command-line argument)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Output path where the results will be saved (args[1] is the second command-line argument)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Launch the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
