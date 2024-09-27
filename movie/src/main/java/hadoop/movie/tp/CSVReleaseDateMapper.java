package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVReleaseDateMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();  
    private boolean isHeader = true;  // Variable pour ignorer l'en-tête (première ligne)

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
        String line = value.toString();
        
        // Ignorer les lignes vides
        if (line == null || line.isEmpty()) {
            return;
        }

        // Ignorer l'en-tête si c'est la première ligne
        if (isHeader) {
            isHeader = false;
            return;
        }

        // Séparer la ligne en colonnes par les virgules
        String[] columns = line.split(",");

        // Vérifier qu'il y a assez de colonnes (au moins 6 pour inclure "release_date")
        if (columns.length > 5) {
            // Supposons que la colonne "release_date" soit la 6ème colonne (indice 5)
            String releaseDate = columns[5].trim();
            
            // Vérifier que la date est bien formatée et qu'elle contient l'année (au moins 10 caractères)
            if (releaseDate.length() == 10 && releaseDate.charAt(2) == '/' && releaseDate.charAt(5) == '/') {
                // Extraire l'année (les 4 derniers caractères de la chaîne)
                String releaseYear = releaseDate.substring(6, 10);
                
                // Définir l'année comme clé
                year.set(releaseYear);
                
                // Emettre l'année avec la valeur 1 pour chaque film
                context.write(year, one);
            }
        }
    }
}
