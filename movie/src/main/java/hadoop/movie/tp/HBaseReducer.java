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
        // Create an HBase configuration from the context's configuration
        Configuration config = HBaseConfiguration.create(context.getConfiguration());

        // Set necessary HBase parameters
        config.set("hbase.zookeeper.quorum", "hadoop-master"); // Replace with your Zookeeper address
        config.set("hbase.zookeeper.property.clientPort", "2181"); // Default port
        // If you have a custom parent znode, uncomment the following line
        // config.set("zookeeper.znode.parent", "/hbase");

        // Initialize HBaseUtils with the HBase configuration
        hbaseUtils = new HBaseUtils(config);

        // Create the table if it does not exist
        hbaseUtils.createTableIfNotExists(TABLE_NAME, COLUMN_FAMILY);

        // Get the reference to the table
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

        // Convert the sum to a string
        String sumAsString = Integer.toString(sum);
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN), Bytes.toBytes(sumAsString));

        table.put(put);

        // If you wish to write to the context
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
