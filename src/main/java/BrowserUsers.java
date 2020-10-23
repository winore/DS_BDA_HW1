import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//User counters
enum Counters {
    TotalLines,     //Counter for total amount of lines
    MalformedLines  //Counter for amount of lines that aren't according to log pattern
}
/*
* Main class of hadoop app
* Finds how many users of specified browser were detected
* */
public class BrowserUsers {
    /*
    * Mapper class
    *
    * */
    public static class BrowserMap extends Mapper<Object, Text, Text, Text>{

        private int malformed = 0, total = 0;       //Counters for malformed and total lines
        private static final Pattern logPattern =   //General pattern of log line
                Pattern.compile("\\d{1,3}([.]\\d{1,3}){3} - - \\[.+\\] \".+\" \\d+ \\d+ \".+\" \".+\"");
        private static final Pattern pBrowser =     //Pattern of User-Agent part of log line
                Pattern.compile("\"[^\"]+?\"$");
        private static final SimpleDateFormat dateFormat = //Pattern for date field
                new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
        /*
        * Check line
        * Return value: if line malformed - null
        *               if not - array of two Strings: IP and log content
        * */
        private String[] isMalformed(String logLine){
            //Check log line is according to general pattern
            if (!logPattern.matcher(logLine).find()){
                return null;
            }

            int end = logLine.indexOf(" - - ");

            //Extract IP from line
            String ip = logLine.substring(0, end);
            //Split IP to bytes
            String[] ipBytes = ip.split("[.]");
            //Check IP is correct
            for (String ipByte : ipBytes) {
                int number = Integer.parseInt(ipByte);
                if (number > 255)
                    return null;
            }
            end += 6;

            //Extract date and time from log line
            String timeStr = logLine.substring(end, end = logLine.indexOf("]", end));
            // Check date and time match template
            try {
                dateFormat.setLenient(false);
                dateFormat.parse(timeStr);
            }
            catch (Exception ex){
                return null;
            }

            //Combine IP and other log content into array of Strings
            String[] returnValues = new String[2];
            returnValues[0] = ip;
            returnValues[1] = logLine.substring(end + 2);

            return returnValues;
        }

        /*
        * Map method for MapReduce process
        * Check the input line isn't malformed, split the line to IP and content,
        * extract from content info about User-Agent and write pair IP,BrowserInfo to output
        * */
        @Override
        public void map(Object o, Text text,
                        Context context) throws IOException, InterruptedException {
            //Increase counter for total lines
            total++;
            String logLine = text.toString();

            //Check log line
            String[] logCont = isMalformed(logLine);
            if (logCont == null){
                //If it is malformed increase counter for malformed lines
                malformed++;
                return;
            }

            //Extract IP
            String ip = logCont[0];
            String content = logCont[1].split("\n")[0];

            //Check User-Agent info
            Matcher m = pBrowser.matcher(content);
            if (m.find()){
                UserAgent uAgent = UserAgent.parseUserAgentString(m.group());
                String browserInfo = uAgent.getBrowser().getName();
                context.write(new Text(browserInfo),new Text(ip));
            }
            else{
                //If it isn't according to pattern increase malformed counter
                malformed++;
            }
        }

        /**
         * Cleanup method for MapReduce process,
         * called on each mapping nodeafter map step is done
         */
        @Override
        public void cleanup(Context context){
            //Increase all counters
            context.getCounter(Counters.TotalLines).increment(total);
            context.getCounter(Counters.MalformedLines).increment(malformed);
        }
    }

    /*
    * Combiner class
    * Count different users of User-Agent
    * */
    public static class BrowserCombine extends Reducer<Text, Text, Text, Text> {

        /*
        * Reduce method
        * Remove duplicate values from input, count remaining values
        * */
        @Override
        public void reduce(Text browser, Iterable<Text> IPs, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            //Create HashSet for removing duplicate values
            HashSet<Text> noDups = new HashSet<>();
            for (Text val : IPs)
                noDups.add(val);
            //Count different values
            sum = noDups.size();
            String sumStr = "";
            sumStr+=sum;
            noDups.clear();
            context.write(new Text(browser), new Text(sumStr));
        }
    }

    /*
    * Reducer Class
    *
    * */
    public static class BrowserReduce extends Reducer<Text, Text, Text, IntWritable> {


        /*
        * Reduce method for MapReduce process
        * */
        @Override
        public void reduce(Text browser, Iterable<Text> IPs, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            //Summarize values from Combiner
            for (Text val : IPs){
                String sumStr = val.toString();
                sum+=Integer.parseInt(sumStr);
            }
            context.write(new Text(browser), new IntWritable(sum));
        }
    }

    /*
    * MapReduce job entry point
    * */
    public static void main(String[] args) throws Exception{

        //Create new configuration
        Configuration conf = new Configuration();
        //Set separator between key and value to comma
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        //Create job names "Browser Users" for configuration
        Job job = Job.getInstance(conf, "Browser Users");

        //Set classes
        job.setJarByClass(BrowserUsers.class);
        job.setMapperClass(BrowserMap.class);
        job.setCombinerClass(BrowserCombine.class);
        job.setReducerClass(BrowserReduce.class);

        //Set output classes for key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);


        //Set input and output paths to paths provided in command line arguments
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //Wait for completion of the job
        if (job.waitForCompletion(true)){
            System.out.println("Total amount of log lines : " + job.getCounters().findCounter(Counters.TotalLines).getValue());
            System.out.println("Amount of malformed lines : " + job.getCounters().findCounter(Counters.MalformedLines).getValue());
            System.exit(0);
        }
        System.exit(1);
    }


}
