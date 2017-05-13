import HbaseImporter.ImportThread;
import org.junit.Test;

/**
 * Created by Administrator on 2017/4/27.
 */
public class ImportThreadTest {
    @Test
    public void importTest() {
        ImportThread importThread = new ImportThread("smb://biggrab:123456@192.168.1.111/biggrab/export/2013-12-09/Exception/Exception.json\n");
        importThread.run();
    }
}
