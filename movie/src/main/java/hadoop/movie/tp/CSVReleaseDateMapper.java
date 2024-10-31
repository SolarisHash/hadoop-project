package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVReleaseDateMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();  
    private boolean isHeader = true;  // Variable to ignore header (first line)

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
        String line = value.toString();
        
        // Ignore empty lines
        if (line == null || line.isEmpty()) {
            return;
        }

        // Ignore the header if it's the first line
        if (isHeader) {
            isHeader = false;
            return;
        }

        // Separate row into columns with commas
        String[] columns = line.split(",");

        // Check that there are enough columns (at least 6 to include “release_date”)
        if (columns.length > 5) {
            // Suppose the “release_date” column is the 6th column (index 5)
            String releaseDate = columns[5].trim();
            
            // Check that the date is correctly formatted and contains the year (at least 10 characters)
            if (releaseDate.length() == 10 && releaseDate.charAt(2) == '/' && releaseDate.charAt(5) == '/') {
                // Extract year (last 4 characters of string)
                String releaseYear = releaseDate.substring(6, 10);
                
                // Define year as key
                year.set(releaseYear);
                
                // Output the year with the value 1 for each film
                context.write(year, one);
            }
        }
    }
}
