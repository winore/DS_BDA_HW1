import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.concurrent.ThreadLocalRandom.*;


public class BrowserMapTest {
    private BrowserUsers.BrowserMap mapper;
    private ArrayList<String> userAgents;
    private ArrayList<String> browsers;
    private ArrayList<String> users;

    private static final String literals = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private enum BadStringType{
        BadPattern,
        BadIP,
        BadDate
    }

    @Before
    public void initTest() {
        mapper = new BrowserUsers.BrowserMap();
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
        browsers = new ArrayList<>();
        users = new ArrayList<>();

    }

    @After
    public void afterTest() {
        mapper = null;
        userAgents.clear();
        userAgents = null;
        browsers.clear();
        browsers = null;
        users.clear();
        users = null;
        System.gc();
    }

    private String getBadIPString(int badByteNumber){
        int badByte = current().nextInt(256,999);
        String result = "";
        for (int i = 0; i < 4; i++){
            if (i == badByteNumber){
                result += badByte;
            }
            else
                result += current().nextInt(255);
            if (i != 3)
                result += '.';
        }
        return result;
    }

    private ArrayList<String> getBadStrings(BadStringType type){
        ArrayList<String> result = new ArrayList<>();
        int linesCount = current().nextInt(1001);
        String badString = "";
        switch (type){
            case BadPattern:
                for (int i = linesCount; i > 0; i--){
                    badString = "";
                    int tmp = current().nextInt(linesCount*10);
                    badString += tmp;
                    result.add(badString);
                }
                break;
            case BadIP:
                for (int i = linesCount; i > 0; i--){
                    badString = "";
                    String IP = getBadIPString(current().nextInt(4));
                    String userAgent = userAgents.get(current().nextInt(userAgents.size()));
                    badString += IP + " - - [" + getGoodDate() +
                            "] \"GET\" 200 765 \"http://www.vk.com/\" \"" +
                            userAgent + '"';
                    result.add(badString);
                }

                break;
            case BadDate:
                //Dates.add("45/Fev/1945:36:78:93 +2583");
                for (int i = linesCount; i > 0; i--){
                    badString = "";
                    String badDate = "";
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
                    result.add(badString);
                }

                break;
            default:
                break;
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
            browsers.add(UserAgent.parseUserAgentString(userAgent).getBrowser().getName());
            users.add(IP);
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
    public void testMalformedLines() throws IOException {
        ArrayList<String> badStrings = getBadStrings(BadStringType.BadPattern);
        badStrings.addAll(getBadStrings(BadStringType.BadIP));
        badStrings.addAll(getBadStrings(BadStringType.BadDate));

        MapDriver<Object,Text,Text,Text> driver = MapDriver.newMapDriver(mapper);
        for (String text : badStrings)
            driver.withInput(NullWritable.get(), new Text(text));

        driver.runTest();
        Assert.assertEquals(badStrings.size(), driver.getCounters().findCounter(Counters.TotalLines).getValue());
        Assert.assertEquals(badStrings.size(), driver.getCounters().findCounter(Counters.MalformedLines).getValue());

    }

    @Test
    public void testGoodLines() throws IOException {
        MapDriver<Object,Text,Text,Text> driver = MapDriver.newMapDriver(mapper);
        ArrayList<String> goodStrings = getGoodStrings();
        for (String text : goodStrings)
            driver.withInput(NullWritable.get(), new Text(text));

        for (int i = 0; i < browsers.size(); i++){
            driver.withOutput(new Text(browsers.get(i)),new Text(users.get(i)));
        }

        driver.runTest();
        Assert.assertEquals(goodStrings.size(), driver.getCounters().findCounter(Counters.TotalLines).getValue());
        Assert.assertEquals(0, driver.getCounters().findCounter(Counters.MalformedLines).getValue());

    }

    @Test
    public void testAllLines() throws IOException {
        MapDriver<Object,Text,Text,Text> driver = MapDriver.newMapDriver(mapper);
        ArrayList<String> goodStrings = getGoodStrings();
        for (String text : goodStrings)
            driver.withInput(NullWritable.get(), new Text(text));

        ArrayList<String> badStrings = getBadStrings(BadStringType.BadPattern);
        badStrings.addAll(getBadStrings(BadStringType.BadIP));
        badStrings.addAll(getBadStrings(BadStringType.BadDate));
        for (String text : badStrings)
            driver.withInput(NullWritable.get(), new Text(text));

        for (int i = 0; i < browsers.size(); i++){
            driver.withOutput(new Text(browsers.get(i)),new Text(users.get(i)));
        }

        driver.runTest();
        Assert.assertEquals(goodStrings.size() + badStrings.size(), driver.getCounters().findCounter(Counters.TotalLines).getValue());
        Assert.assertEquals(badStrings.size(), driver.getCounters().findCounter(Counters.MalformedLines).getValue());

    }
}
