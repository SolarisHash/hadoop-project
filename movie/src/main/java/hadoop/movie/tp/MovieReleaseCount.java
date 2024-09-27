package hadoop.movie.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// Importez cette classe pour la sortie NullOutputFormat
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

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
        // Si vous souhaitez utiliser un combiner, assurez-vous qu'il est compatible avec le Reducer
        // job.setCombinerClass(IntSumReducer.class);
        
        // Définir le Reducer à utiliser (HBaseReducer)
        job.setReducerClass(HBaseReducer.class);
        
        // Définir les classes de types de sortie pour les clés et valeurs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Chemin d'entrée du fichier CSV
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Si vous ne souhaitez pas générer de sortie dans HDFS (puisque les données sont insérées dans HBase),
        // vous pouvez utiliser NullOutputFormat
        job.setOutputFormatClass(NullOutputFormat.class);

        // Lancer le job et attendre sa complétion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
