import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); 

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable")); 

    Put put = new Put(Bytes.toBytes("key1")); 
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("a"), Bytes.toBytes("val1")); 
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("b"), Bytes.toBytes("val2")); 
    table.put(put); 

    put = new Put(Bytes.toBytes("key2")); 
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("a"), Bytes.toBytes("val3")); 
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c"), Bytes.toBytes("val4")); 

    table.put(put); 
    table.close(); 
    connection.close();

    System.out.println("Put is completed");
  }
}
