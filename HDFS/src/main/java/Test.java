import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @program: Hadoop->PACKAGE_NAME->Test
 * @description: 练习测试
 * @author: Mr.黄
 * @create: 2019-10-13 22:14
 **/
public class Test {
    public static void main(String[] args) {
        Text t = new Text("lisi");
        List<Text> list = new ArrayList<Text>();
        list.add(t);
        t.set("wangwu");
        list.add(t);
        t.set("zhangsan");
        list.add(t);
        for (Text text : list) {
            System.out.println(text);
        }
    }

    @org.junit.Test
    public void test() {

    }
}
