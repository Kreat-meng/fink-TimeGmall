import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;

/**
 * @author MengX
 * @create 2023/3/10 08:30:47
 */
public class Test {

    public static void main(String[] args) {

        HashMap<String, String> string = new HashMap<>();

        string.put("lalal","nnn");

        string.put("huauhua","aaa");

        JSONObject jsonObject = new JSONObject().fluentPutAll(string);

        System.out.println(jsonObject);
    }
}
