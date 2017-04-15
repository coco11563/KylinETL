package HbaseImporter;

import net.sf.json.JSONObject;

/**
 * Created by Sha0w on 2017/4/15.
 */
public class OtherInform {
    private String content;
    private String gender;
    private String picURL;
    private String otherInform;
    private String username;
    public OtherInform(String json) {
        setOtherInform(json);
        JSONObject jsonObject = JSONObject.fromObject(json);
        if(jsonObject.containsKey("bmiddle_pic")) {
            setPicURL(jsonObject.getString("bmiddle_pic"));
        } else {
            setPicURL("0");
        }
        setGender(jsonObject.getJSONObject("user").getString("gender"));
        setUsername(jsonObject.getJSONObject("user").getString("name"));
        setContent(jsonObject.getString("text"));
    }
    @Override
    public String toString(){
        return getOtherInform();
    }
    public String getOtherInform() {
        return otherInform;
    }

    public void setOtherInform(String otherInform) {
        this.otherInform = otherInform;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getPicURL() {
        return picURL;
    }

    public void setPicURL(String picURL) {
        this.picURL = picURL;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
