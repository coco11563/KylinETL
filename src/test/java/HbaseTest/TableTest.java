package HbaseTest;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.Test;

import java.io.IOException;

import static HbaseImporter.HbaseImporter.cfg;

/**
 * Created by Administrator on 2017/4/16.
 */
public class TableTest {
    @Test
    public void TableTest() throws IOException {
//        HTablePool pool = new HTablePool(cfg, 1000);
//        HTable table = (HTable) pool.getTable("SinaWeioDataStorage");
//        table.toString();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(cfg);
        TableName[] tn = hBaseAdmin.listTableNames();
        for (TableName t : tn) {
            System.out.println(t.getNameAsString());
        }
    }
}
