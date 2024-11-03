
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); 

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable")); 

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    int numOfRows = 0;
    try (ResultScanner scanner = table.getScanner(scan)){
        for( Result result : scanner){
            numOfRows++;
        } 
    }
    System.out.println("Number of rows : " + numOfRows);

    Delete delete = new Delete(Bytes.toBytes("key1"));
    delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("b")); 
    table.delete(delete);

    delete = new Delete(Bytes.toBytes("key2")); 
    table.delete(delete); 
    table.close(); 
    connection.close();

    System.out.println("Delete is completed");
  }
}
