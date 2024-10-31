package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVReleaseDateMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();  
    private boolean isHeader = true;  // Variable to skip the header (first line)

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
        String line = value.toString();
        
        // Skip empty lines
        if (line == null || line.isEmpty()) {
            return;
        }

        // Skip the header if it's the first line
        if (isHeader) {
            isHeader = false;
            return;
        }

        // Split the line into columns by commas
        String[] columns = line.split(",");

        // Check that there are enough columns (at least 6 to include "release_date")
        if (columns.length > 5) {
            // Assume that the "release_date" column is the 6th column (index 5)
            String releaseDate = columns[5].trim();
            
            // Check that the date is properly formatted and contains the year (exactly 10 characters)
            if (releaseDate.length() == 10 && releaseDate.charAt(2) == '/' && releaseDate.charAt(5) == '/') {
                // Extract the year (the last 4 characters of the string)
                String releaseYear = releaseDate.substring(6, 10);
                
                // Set the year as the key
                year.set(releaseYear);
                
                // Emit the year with the value 1 for each movie
                context.write(year, one);
            }
        }
    }
}
