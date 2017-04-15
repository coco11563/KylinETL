package HbaseImporter;

import HbaseImporter.ConfigurePart.Inial;
import datastruct.KeySizeException;
import net.sf.json.JSONObject;

import java.text.ParseException;

/**
 * Created by root on 1/12/17.
 *
 *
 */
public class HbaseCeller {
    private rowKey rowKey;
    private OtherInform otherInform;
    public static Inial inial;

    static {
        inial = new Inial();
    }
    public HbaseCeller(JSONObject jsonObject) throws ParseException, KeySizeException {
        this.rowKey = new rowKey(jsonObject, inial);
        this.otherInform = new OtherInform(jsonObject.toString());
    }

    public HbaseCeller(String jsonObject) throws ParseException, KeySizeException {
        this.rowKey = new rowKey(JSONObject.fromObject(jsonObject), inial);
        this.otherInform = new OtherInform(jsonObject);
    }

    public rowKey getRowKey() {
        return rowKey;
    }

    public void setRowKey(rowKey rowKey) {
        this.rowKey = rowKey;
    }

    public OtherInform getOtherInform() {
        return otherInform;
    }

    public void setOtherInform(OtherInform otherInform) {
        this.otherInform = otherInform;
    }




}