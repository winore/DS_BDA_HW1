import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

public class BrowserCombineTest {
    private BrowserUsers.BrowserCombine combiner;
    private HashSet<String> browsers;

    @Before
    public void initTest(){
        combiner = new BrowserUsers.BrowserCombine();
        browsers = new HashSet<>();
        Browser[] brs = Browser.values();
        for (int i = 0; i <  brs.length; i++)
            browsers.add(brs[i].getName());
    }

    @After
    public void afterTest(){
        combiner = null;
        browsers.clear();
        browsers = null;
        System.gc();
    }

    private String getGoodIP(){
        String result = "";
        for (int i = 0; i < 4; i++){
            int ipByte = ThreadLocalRandom.current().nextInt(255);
            result+=ipByte;
            if (i != 3)
                result+='.';
        }
        return result;
    }

    private HashSet<Text> differentUsers(){
        HashSet<Text> result = new HashSet<>();
        int size = ThreadLocalRandom.current().nextInt(1,1025);
        for (int i = size; i > 0; i--)
            result.add(new Text(getGoodIP()));
        return result;
    }

    private ArrayList<Text> sameUsers(){
        ArrayList<Text> result = new ArrayList<>();
        int size = ThreadLocalRandom.current().nextInt(1,1025);
        String IP = getGoodIP();
        for (int i = size; i > 0; i--)
            result.add(new Text(IP));
        return result;
    }

    private ArrayList<Text> allUsers(HashSet<Text> differentUsers){
        ArrayList<Text> result = new ArrayList<>(differentUsers);
        int size = differentUsers.size();
        int count = ThreadLocalRandom.current().nextInt(1,size + 1);
        for (; count > 0; count--){
            int i = ThreadLocalRandom.current().nextInt(size);
            int times = ThreadLocalRandom.current().nextInt(21);
            for (; times > 0; times--)
                result.add(result.get(i));
        }
        return result;
    }

    @Test
    public void testSameUsers() throws IOException {
        ReduceDriver <Text, Text, Text, Text> driver = ReduceDriver.newReduceDriver(combiner);
        for (String browser : browsers){
            driver.withInput(new Text(browser), sameUsers());
            driver.withOutput(new Text(browser), new Text("1"));
        }
        driver.runTest();
    }

    @Test
    public void testDifferentUsers() throws IOException {
        ReduceDriver <Text, Text, Text, Text> driver = ReduceDriver.newReduceDriver(combiner);
        for (String browser : browsers){
            ArrayList<Text> users = new ArrayList<>(differentUsers());
            String size = ""; size += users.size();
            driver.withInput(new Text(browser), users);
            driver.withOutput(new Text(browser), new Text(size));
        }
        driver.runTest();
    }

    @Test
    public void testAllUsers() throws IOException {
        ReduceDriver <Text, Text, Text, Text> driver = ReduceDriver.newReduceDriver(combiner);
        for (String browser : browsers){
            HashSet<Text> dUsers = differentUsers();
            String size = ""; size += dUsers.size();
            driver.withInput(new Text(browser), allUsers(dUsers));
            driver.withOutput(new Text(browser), new Text(size));
        }
        driver.runTest();
    }
}
