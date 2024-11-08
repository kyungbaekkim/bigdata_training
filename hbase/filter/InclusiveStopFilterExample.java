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
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class InclusiveStopFilterExample{
  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-100"));

    Filter filter1 = new InclusiveStopFilter( Bytes.toBytes("row-5"));

    scan.setStartRow(Bytes.toBytes("row-3"));
    scan.setFilter(filter1);
    ResultScanner scanner1 = table.getScanner(scan);
    System.out.println("Scanning table #1... row-3 ~ row-5");
    for (Result res : scanner1) {
      for (Cell cell : res.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " + 
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
    }
    scanner1.close();

  }
}
