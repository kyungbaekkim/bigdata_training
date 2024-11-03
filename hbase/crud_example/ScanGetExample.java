import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class ScanGetExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); 

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable")); 

    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner){
        System.out.println("Found row: " + result);
    }
    scanner.close();
    System.out.println("Scan is completed");

    Get get = new Get(Bytes.toBytes("key1"));
    Result result = table.get(get);

    byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("a"));
    System.out.println("cf1:a - Key : key1,"+" Retrived value : "+Bytes.toString(value));
    value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("b"));
    System.out.println("cf1:b - Key : key1,"+" Retrived value : "+Bytes.toString(value));

    get = new Get(Bytes.toBytes("key2"));
    result = table.get(get);
    System.out.println(result);

    table.close(); 
    connection.close();

    System.out.println("Get is completed");
  }
}
