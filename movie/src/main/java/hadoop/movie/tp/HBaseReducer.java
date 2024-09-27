package hadoop.movie.tp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
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
        Configuration config = context.getConfiguration();
        hbaseUtils = new HBaseUtils(config);
        hbaseUtils.createTableIfNotExists(TABLE_NAME, COLUMN_FAMILY);
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
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN), Bytes.toBytes(sum));

        table.put(put);

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
