import java.io.IOException;

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
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class ValueFilterExample{
  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    Scan scan = new Scan();

    Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, 
      new SubstringComparator(".4"));
    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    System.out.println("Scanning table #1...equal .4");
    for (Result res : scanner1) {
      for (Cell cell : res.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " + 
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
    }
    scanner1.close();

    Get get1 = new Get(Bytes.toBytes("row-5"));
    get1.setFilter(filter1); 
    Result result1 = table.get(get1);
    System.out.println("###############################");
    System.out.println("Result of Get(): ");
      for (Cell cell : result1.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " + 
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }

  }
}
