import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class RowFilterExample{
  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"));

    Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, 
      new BinaryComparator(Bytes.toBytes("row-22")));
    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    System.out.println("Scanning table #1...less or equal row-22");
    for (Result res : scanner1) {
      System.out.println(res);
    }
    scanner1.close();

    Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, 
      new RegexStringComparator(".*-.5"));
    scan.setFilter(filter2);
    ResultScanner scanner2 = table.getScanner(scan);
    System.out.println("Scanning table #2... equal [.*-.5] regular expression");
    for (Result res : scanner2) {
      System.out.println(res);
    }
    scanner2.close();

    Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, 
      new SubstringComparator("-5"));
    scan.setFilter(filter3);
    ResultScanner scanner3 = table.getScanner(scan);
    System.out.println("Scanning table #3... equal substring {-5}");
    for (Result res : scanner3) {
      System.out.println(res);
    }
    scanner3.close();

  }
}
