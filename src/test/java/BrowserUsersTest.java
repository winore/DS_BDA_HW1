import com.google.common.collect.HashMultimap;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.eclipse.jetty.util.MultiMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.concurrent.ThreadLocalRandom.current;

public class BrowserUsersTest {
    private BrowserUsers.BrowserMap mapper;
    private BrowserUsers.BrowserCombine combiner;
    private BrowserUsers.BrowserReduce reducer;

    private ArrayList<String> userAgents;
    private HashMultimap<String, String> differentUsers;

    private static final String literals = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    @Before
    public void initTest(){
        mapper = new BrowserUsers.BrowserMap();
        combiner = new BrowserUsers.BrowserCombine();
        reducer = new BrowserUsers.BrowserReduce();

        userAgents = new ArrayList<>();
        userAgents.add("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0");
        userAgents.add("Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0");
        userAgents.add("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36");
        userAgents.add("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41");
        userAgents.add("Opera/9.80 (Macintosh; Intel Mac OS X; U; en) Presto/2.2.15 Version/10.00");
        userAgents.add("Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1");
        userAgents.add("Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1");
        userAgents.add("Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)");
        userAgents.add("Googlebot/2.1 (+http://www.google.com/bot.html)");
        differentUsers = HashMultimap.create();
    }

    @After
    public void afterTest(){
        mapper = null;
        combiner = null;
        reducer = null;

        userAgents.clear();
        userAgents = null;
        differentUsers.clear();
        differentUsers = null;

        System.gc();
    }

    private String getBadIPString(){
        int badByteNumber = current().nextInt(4);
        int badByte = current().nextInt(256,999);
        String IP = "";
        for (int i = 0; i < 4; i++){
            if (i == badByteNumber){
                IP += badByte;
            }
            else
                IP += current().nextInt(255);
            if (i != 3)
                IP += '.';
        }

        String badString = "";
        String userAgent = userAgents.get(current().nextInt(userAgents.size()));
        badString += IP + " - - [" + getGoodDate() +
                "] \"GET\" 200 765 \"http://www.vk.com/\" \"" +
                userAgent + '"';

        return badString;
    }

    private String getBadPatternString(){
        String badString = "";
        int tmp = current().nextInt(10241);
        badString += tmp;
        return badString;
    }

    private String getBadDateString(){
        String badString = "", badDate = "";
        int badDay = current().nextInt(32, 45);
        String badMonth = "";
        badMonth += literals.charAt(current().nextInt(literals.length()));
        badMonth += literals.charAt(current().nextInt(literals.length()));
        badMonth += literals.charAt(current().nextInt(literals.length()));
        int year = current().nextInt(1970, 2123);
        int hour = current().nextInt(25,100);
        int min = current().nextInt(61,100);
        int sec = current().nextInt(61,100);
        int zone = current().nextInt(1001,10000);
        badDate = String.format("%d/%s/%d:%d:%d:%d +%d",
                badDay, badMonth,year,hour,min,sec,zone);

        String IP = getGoodIP();
        String userAgent = userAgents.get(current().nextInt(userAgents.size()));
        badString += IP + " - - [" + badDate +
                "] \"GET\" 200 765 \"http://www.vk.com/\" \"" +
                userAgent + '"';
        return badString;
    }

    private ArrayList<String> getBadStrings(){
        ArrayList<String> result = new ArrayList<>();
        int linesCount = current().nextInt(1,10001);
        String badString = "";
        for (; linesCount > 0; linesCount--){
            int type = current().nextInt(0,2);
            switch (type){
                case 0:
                    result.add(getBadPatternString());
                    break;
                case 1:
                    result.add(getBadIPString());
                    break;
                case 2:
                    result.add(getBadDateString());
                    break;

            }
        }
        return result;
    }

    private ArrayList<String> getGoodStrings(){
        ArrayList<String> result = new ArrayList<>();
        int resultSize = current().nextInt(1,10000);
        for (int i = resultSize; i > 0; i--){
            String goodString = "";
            String IP = getGoodIP();
            String userAgent = userAgents.get(current().nextInt(userAgents.size()));
            goodString += IP + " - - [" + getGoodDate() +
                    "] \"GET\" 200 765 \"http://www.vk.com/\" \"" +
                    userAgent + '"';
            result.add(goodString);
            differentUsers.put(UserAgent.parseUserAgentString(userAgent).getBrowser().getName(), IP);
        }
        return result;
    }

    private String getGoodIP(){
        String result = "";
        for (int i = 0; i < 4; i++){
            int ipByte = current().nextInt(255);
            result+=ipByte;
            if (i != 3)
                result+='.';
        }
        return result;
    }

    private String getGoodDate(){
        String result = "";
        long now = Calendar.getInstance().getTimeInMillis();
        long dateLong = current().nextLong(now - 300000000L * 1000, now);
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

        Date date = new Date(dateLong);
        result = sdf.format(date);
        return result;
    }

    @Test
    public void test() throws IOException {
        MapReduceDriver<Object,Text,Text,Text,Text,IntWritable> driver =
                MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);

        ArrayList<String> goodStrings = getGoodStrings();
        for (String text : goodStrings)
            driver.withInput(NullWritable.get(), new Text(text));

        ArrayList<String> badStrings = getBadStrings();
        for (String text : badStrings)
            driver.withInput(NullWritable.get(), new Text(text));

        TreeSet<String> keys = new TreeSet<>(differentUsers.keySet());
        for (String key : keys){
            Set<String> IPs = differentUsers.get(key);
            driver.withOutput(new Text(key), new IntWritable(IPs.size()));
        }
        driver.runTest();
        Assert.assertEquals(goodStrings.size() + badStrings.size(), driver.getCounters().findCounter(Counters.TotalLines).getValue());
        Assert.assertEquals(badStrings.size(), driver.getCounters().findCounter(Counters.MalformedLines).getValue());
    }
}
