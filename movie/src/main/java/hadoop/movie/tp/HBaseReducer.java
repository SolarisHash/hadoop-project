package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private HBaseUtils hbaseUtils;
    private Table table;
    private static final String TABLE_NAME = "movie_release_counts";
    private static final String COLUMN_FAMILY = "counts";
    private static final String COLUMN = "count";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Créer une configuration HBase à partir de la configuration du contexte
        Configuration config = HBaseConfiguration.create(context.getConfiguration());

        // Définir les paramètres HBase nécessaires
        config.set("hbase.zookeeper.quorum", "hadoop-master"); // Remplacez par l'adresse de votre Zookeeper
        config.set("hbase.zookeeper.property.clientPort", "2181"); // Port par défaut
        // Si vous avez un znode parent personnalisé, décommentez la ligne suivante
        // config.set("zookeeper.znode.parent", "/hbase");

        // Initialiser HBaseUtils avec la configuration HBase
        hbaseUtils = new HBaseUtils(config);

        // Créer la table si elle n'existe pas
        hbaseUtils.createTableIfNotExists(TABLE_NAME, COLUMN_FAMILY);

        // Obtenir la référence à la table
        table = hbaseUtils.getTable(TABLE_NAME);
    }


    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        String year = key.toString();
        Put put = new Put(Bytes.toBytes(year));

        // Convertir la somme en chaîne de caractères
        String sumAsString = Integer.toString(sum);
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN), Bytes.toBytes(sumAsString));

        table.put(put);

        // Si vous souhaitez écrire dans le contexte
        context.write(key, new IntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (table != null) {
            table.close();
        }
        if (hbaseUtils != null) {
            hbaseUtils.close();
        }
    }


}
