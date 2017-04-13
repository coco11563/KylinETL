package StarModelBuilder.User;

/**
 * Created by Sha0w on 2017/4/13.
 */
public class User {
    private String user_id;
    private String gender;
    public User(String user_id, String gender) {
        setGender(gender);
        setUser_id(user_id);
    }
    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }
}
