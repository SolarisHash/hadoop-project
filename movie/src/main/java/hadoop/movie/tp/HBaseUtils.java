package hadoop.movie.tp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;

import java.io.IOException;

public class HBaseUtils {
    private Connection connection;
    private Admin admin;

    public HBaseUtils(Configuration config) throws IOException {
        this.connection = ConnectionFactory.createConnection(config);
        this.admin = connection.getAdmin();
    }

    public void createTableIfNotExists(String tableNameStr, String columnFamily) throws IOException {
        TableName tableName = TableName.valueOf(tableNameStr);
        if (!admin.tableExists(tableName)) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily))
                    .build();
            admin.createTable(tableDescriptor);
        }
    }

    public Table getTable(String tableNameStr) throws IOException {
        return connection.getTable(TableName.valueOf(tableNameStr));
    }

    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
