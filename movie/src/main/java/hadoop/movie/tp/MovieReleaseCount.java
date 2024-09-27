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
        // Vérifier que deux arguments (chemin d'entrée et de sortie) sont passés
        if (args.length != 2) {
            System.err.println("Usage: MovieReleaseCount <input path> <output path>");
            System.exit(-1);
        }

        // Configuration de base pour le job MapReduce
        Configuration conf = new Configuration();
        
        // Création d'une nouvelle instance de job
        Job job = Job.getInstance(conf, "movie release year count");
        
        // Définir la classe qui contient le point d'entrée (main) du programme
        job.setJarByClass(MovieReleaseCount.class);
        
        // Définir le Mapper à utiliser (CSVReleaseDateMapper)
        job.setMapperClass(CSVReleaseDateMapper.class);
        
        // Le combiner (facultatif) utilise également le Reducer dans ce cas pour optimiser la performance
        job.setCombinerClass(IntSumReducer.class);
        
        // Définir le Reducer à utiliser (IntSumReducer)
        job.setReducerClass(IntSumReducer.class);
        
        // Définir les classes de types de sortie pour les clés et valeurs
        job.setOutputKeyClass(Text.class);  // La clé de sortie est de type Text (l'année de sortie)
        job.setOutputValueClass(IntWritable.class);  // La valeur de sortie est de type IntWritable (le nombre de films)
        
        // Chemin d'entrée du fichier CSV (args[0] est le premier argument en ligne de commande)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Chemin de sortie où les résultats seront enregistrés (args[1] est le deuxième argument en ligne de commande)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Lancer le job et attendre sa complétion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
