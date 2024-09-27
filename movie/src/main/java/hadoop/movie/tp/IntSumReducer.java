package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable(); // Stocke la somme des films par année
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0; // Initialiser la somme à 0
        
        // Parcourir toutes les valeurs associées à la clé (qui est l'année dans ce cas)
        for (IntWritable val : values) {
            sum += val.get(); // Additionner toutes les occurrences pour l'année
        }
        
        // Assigner la somme à la variable result
        result.set(sum);
        
        // Écrire l'année (key) et la somme (nombre de films) en sortie
        context.write(key, result);
    }
}
