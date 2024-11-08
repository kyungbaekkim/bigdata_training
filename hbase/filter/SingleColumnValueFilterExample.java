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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnValueFilterExample{
  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-100"));

    SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
        Bytes.toBytes("colfam1"), Bytes.toBytes("col-100"),
        CompareFilter.CompareOp.EQUAL,
        new SubstringComparator("val-5"));
    filter1.setFilterIfMissing(true);

    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    System.out.println("Scanning table #1...equal colfam1, col-100, val-5");
    for (Result res : scanner1) {
      for (Cell cell : res.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " + 
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
    }
    scanner1.close();

    Get get1 = new Get(Bytes.toBytes("row-59"));
    get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-100"));
    get1.setFilter(filter1); 
    Result result1 = table.get(get1);
    System.out.println("###############################");
    System.out.println("Result of Get(): row-59 ");
      for (Cell cell : result1.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " + 
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }

  }
}
