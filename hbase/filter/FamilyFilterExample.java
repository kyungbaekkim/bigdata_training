import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class FamilyFilterExample{
  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    Scan scan = new Scan();

    Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS, 
      new BinaryComparator(Bytes.toBytes("colfam2")));
    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    System.out.println("Scanning table #1...less colfam2");
    for (Result res : scanner1) {
      System.out.println(res);
    }
    scanner1.close();

    Get get1 = new Get(Bytes.toBytes("row-5"));
    get1.setFilter(filter1); 
    Result result1 = table.get(get1);
    System.out.println("###############################");
    System.out.println("Result of Get(): " + result1);

    Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes("colfam3")));
    Get get2 = new Get(Bytes.toBytes("row-5")); 
    get2.addFamily(Bytes.toBytes("colfam1"));
    get2.setFilter(filter2);
    Result result2 = table.get(get2); 
    System.out.println("###############################");
    System.out.println("Result of get(): " + result2);
  }
}
