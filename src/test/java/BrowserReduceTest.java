import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

public class BrowserReduceTest {
    private BrowserUsers.BrowserReduce reducer;
    private HashSet<String> browsers;

    @Before
    public void initTest() {
        reducer = new BrowserUsers.BrowserReduce();
        browsers = new HashSet<>();
        Browser[] brs = Browser.values();
        for (int i = 0; i <  brs.length; i++)
            browsers.add(brs[i].getName());
    }

    @After
    public void afterTest() {
        reducer = null;
        browsers.clear();
        browsers = null;
        System.gc();
    }

    private ArrayList<Text> fromOneMapper(int count){
        ArrayList<Text> result = new ArrayList<>();
        String countStr = "";
        countStr += count;
        result.add(new Text(countStr));
        return result;
    }

    private ArrayList<Text> fromFewMappers(int[] values){
        ArrayList<Text> result = new ArrayList<>();
        int size = values.length;
        for (int value : values) {
            String countStr = "";
            countStr += value;
            result.add(new Text(countStr));
        }

        return result;
    }

    @Test
    public void testFromOneMapper() throws IOException {
        ReduceDriver<Text, Text, Text, IntWritable> driver =
                ReduceDriver.newReduceDriver(reducer);
        for (String browser : browsers){
            int count = ThreadLocalRandom.current().nextInt(1,46);
            driver.withInput(new Text(browser), fromOneMapper(count));
            driver.withOutput(new Text(browser), new IntWritable(count));
        }
        driver.runTest();
    }

    @Test
    public void testFromFewMappers() throws IOException {
        ReduceDriver<Text, Text, Text, IntWritable> driver =
                ReduceDriver.newReduceDriver(reducer);
        for (String browser : browsers){
            int count = ThreadLocalRandom.current().nextInt(1,51);
            int sum = 0;
            int[] values = new int[count];
            for (int i = 0; i < count; i++){
                values[i] = ThreadLocalRandom.current().nextInt(1,46);
                sum += values[i];
            }
            driver.withInput(new Text(browser), fromFewMappers(values));
            driver.withOutput(new Text(browser), new IntWritable(sum));
        }
        driver.runTest();
    }
}
