
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import java.io.IOException;

public class DisableDropExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); 

    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin(); 

    List<TableDescriptor> tableDescs = admin.listTableDescriptors();
    for (TableDescriptor tableDesc : tableDescs ){
        System.out.println("Table Name: " + tableDesc.getTableName().getNameAsString());
    }

    TableName tableName = TableName.valueOf("testtable");
    if( admin.tableExists(tableName) ){
        if( admin.isTableEnabled(tableName) ){
            admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
    } 
    connection.close();
    System.out.println("testtable is deleted");
  }
}
