
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {
    private static final String HBASE_CONF_DIR = System.getenv("HBASE_CONF_DIR");
    private static final String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");


    private static void createTable(Configuration config, Admin admin) throws IOException {
        // Create table if it is not created
        TableName tableName = TableName.valueOf("tourists");

        if (admin.tableExists(tableName)) {
            System.out.println("Found previous version of table 'tourists'. Table will be deleted and repopulated.");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        
        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor columnFamilyOrigin = new HColumnDescriptor("origin");
        HColumnDescriptor columnFamilyEntry = new HColumnDescriptor("entry");

        table.addFamily(columnFamilyOrigin.setCompressionType(Algorithm.NONE));
        table.addFamily(columnFamilyEntry.setCompressionType(Algorithm.NONE));

        admin.createTable(table);

        System.out.println("Table 'tourists' created successfully.");
    }


    public static void populateTable(Configuration config, Admin admin, Connection conn) throws IOException {
        // Load data from data/touristData.csv
        Table table = conn.getTable(TableName.valueOf("tourists"));

        String csvFile = "data/touristData.csv";

        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        Integer numberOfRows = 1;
        String line = "";

        while ((line = reader.readLine()) != null) {
            String [] splitted = line.split(",");

            String continent = splitted[0];
            String country = splitted[1];
            String state = splitted[2];
            String wayIn = splitted[3];
            String year = splitted[4];
            String month = splitted[5];
            String count = splitted[6];

            // Store data in table
            Put p = new Put(Bytes.toBytes("row_" + numberOfRows));
            p.addColumn(Bytes.toBytes("origin"), Bytes.toBytes("continent"), Bytes.toBytes(continent));
            p.addColumn(Bytes.toBytes("origin"), Bytes.toBytes("country"), Bytes.toBytes(country));
            p.addColumn(Bytes.toBytes("entry"), Bytes.toBytes("state"), Bytes.toBytes(state));
            p.addColumn(Bytes.toBytes("entry"), Bytes.toBytes("wayin"), Bytes.toBytes(wayIn));
            p.addColumn(Bytes.toBytes("entry"), Bytes.toBytes("year"), Bytes.toBytes(year));
            p.addColumn(Bytes.toBytes("entry"), Bytes.toBytes("month"), Bytes.toBytes(month));
            p.addColumn(Bytes.toBytes("entry"), Bytes.toBytes("count"), Bytes.toBytes(count));

            table.put(p);

            numberOfRows++;
        }

        reader.close();
        table.close();

        System.out.println("Table 'tourists' populated with " + numberOfRows + " rows.");
    }


    public static void main(String [] args) {

        try {
            // Connect to HBase
            Configuration config = HBaseConfiguration.create();

            config.addResource(new Path(HBASE_CONF_DIR + "/hbase-site.xml"));
            config.addResource(new Path(HADOOP_CONF_DIR + "/core-site.xml"));

            Connection conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();

            createTable(config, admin);
            populateTable(config, admin, conn);

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}