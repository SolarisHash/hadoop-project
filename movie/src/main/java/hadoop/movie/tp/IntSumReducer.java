package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable(); // Stores the sum of films by year
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0;
        
        // Browse all values associated with the key (which in this case is the year)
        for (IntWritable val : values) {
            sum += val.get(); // Add up all occurrences for the year
        }
        
        // Assign the sum to the result variable
        result.set(sum);
        
        // Write the year (key) and the sum (number of films) as output
        context.write(key, result);
    }
}
