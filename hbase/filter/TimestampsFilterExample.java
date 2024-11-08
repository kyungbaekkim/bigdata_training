import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class TimestampsFilterExample{
  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    List<Long> ts = new ArrayList<Long>();
    ts.add(new Long(1731059786092L));
    ts.add(new Long(1731059789629L));

    Filter filter1 = new TimestampsFilter( ts );

    Scan scan = new Scan();
    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    System.out.println("Scanning table #1... ts match");
    for (Result result : scanner1) {
        System.out.println(result);
    }
    scanner1.close();

    Scan scan2 = new Scan();
    scan2.setTimeRange(1731059786090L, 1731059786120L); 
    ResultScanner scanner2 = table.getScanner(scan2);
    System.out.println("Results of scan #2: ts range");
    for (Result result : scanner2) {
      System.out.println(result);
    }
    scanner2.close();


  }
}
