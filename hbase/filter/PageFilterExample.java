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
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class PageFilterExample{
  private static final byte[] POSTFIX = new byte[] { 0x00 };

  public static void main(String[] args) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("random_table"));

    PageFilter filter = new PageFilter( 15 );

    int totalRows = 0;
    byte[] lastRow = null;
    while (true) {
      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-100"));

      scan.setFilter(filter);
      if (lastRow != null) {
        byte[] startRow = Bytes.add(lastRow, POSTFIX);
        System.out.println("start row: " +
          Bytes.toStringBinary(startRow));
        scan.setStartRow(startRow);
      }
      ResultScanner scanner = table.getScanner(scan);
      int localRows = 0;
      Result result;
      while ((result = scanner.next()) != null) {
        System.out.println(localRows++ + ": " + result);
        totalRows++;
        lastRow = result.getRow();
      }
      scanner.close();
      if (localRows == 0) break;
    }
    System.out.println("total rows: " + totalRows);

  }
}
