package MapReduce.PageRank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by lubuntu on 16-7-11.
 */
public class PageRankIter {


    // Loop ends with the condition of times

    static class PageRankIterMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text keyOut = new Text();
            Text valueOut = new Text();
            String[] lists = value.toString().split(",");
            double pr = Double.parseDouble(lists[0]);
            for(int i = 1; i < lists.length; ++i) {
                keyOut.set(lists[i]);
                valueOut.set("src," + key.toString() + "," + String.valueOf(pr / (lists.length - 1)));
                context.write(keyOut, valueOut);
            }
            int firstCommaIndex = value.toString().indexOf(',');
            valueOut.set(value.toString().substring(firstCommaIndex + 1));
            context.write(key, valueOut);
        }
    }

    static class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {

        private static double d = 0.85;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double pr = 0;
            StringBuilder strBuilder = new StringBuilder("");
            for(Text value: values) {
                String[] lists = value.toString().split(",");
                if (lists[0].equals("src")) {
                    pr += d * Double.parseDouble(lists[2]);
                } else {
                    pr += 1 - d;
                    strBuilder.append(value.toString());
                }
            }
            Text valueOut = new Text(String.valueOf(pr) + "," + strBuilder.toString());
            context.write(key, valueOut);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("usage: PageRankIter <in> <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job4_PageRank.PageRankIter");
            job.setJarByClass(PageRankIter.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(PageRankIterMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(PageRankIterReducer.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /* Still has some bugs
    // Loop ends with the condition of threshold

    static class PageRankIterMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text keyOut = new Text();
            Text valueOut = new Text();
            String[] lists = value.toString().split(",");
            double pr = Double.parseDouble(lists[0]);
            for(int i = 1; i < lists.length; ++i) {
                keyOut.set(lists[i]);
                valueOut.set("src," + key.toString() + "," + String.valueOf(pr / (lists.length - 1)));
                context.write(keyOut, valueOut);
            }
            valueOut.set(value.toString());
            context.write(key, valueOut);
        }
    }

    static class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {

        private static double d = 0.85;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double pr = 0, oldPr = 0;
            StringBuilder strBuilder = new StringBuilder("");
            for(Text value: values) {
                String[] lists = value.toString().split(",");
                if (lists[0].equals("src")) {
                    pr += d * Double.parseDouble(lists[2]);
                } else {
                    pr += 1 - d;
                    String valueStr = value.toString();
                    int firstCommaIndex = valueStr.indexOf(',');
                    oldPr = Double.parseDouble(valueStr.substring(0, firstCommaIndex));
                    strBuilder.append(valueStr.substring(firstCommaIndex + 1));
                }
            }
            double deltaSum = Double.parseDouble(context.getConfiguration().get("deltaSum")) + Math.abs(pr - oldPr);
            context.getConfiguration().set("deltaSum", String.valueOf(deltaSum));
            double totalNum = Double.parseDouble(context.getConfiguration().get("totalNum")) + 1;
            context.getConfiguration().set("totalNum", String.valueOf(totalNum));
            Text valueOut = new Text(String.valueOf(pr) + "," + strBuilder.toString());
            context.write(key, valueOut);
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("usage: PageRankIter <in> <out> <theta>");
            System.exit(2);
        }
        double theta = Double.parseDouble(args[2]);
        double sigma = Double.MAX_VALUE;
        int i = 0;
        while(sigma >= theta) {
            try {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                Job job = new Job(conf, "JinYongsJiangHu_Job4_PageRank.PageRankIter");
                job.setJarByClass(PageRankIter.class);
                job.getConfiguration().set("deltaSum", String.valueOf(0));
                job.getConfiguration().set("totalNum", String.valueOf(0));
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setMapperClass(PageRankIterMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(PageRankIterReducer.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0] + String.valueOf(i)));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[0] + String.valueOf(i + 1)));
                while (! job.waitForCompletion(true)) ;
                {
                    double deltaSum = Double.parseDouble(job.getConfiguration().get("deltaSum"));
                    double totalNum = Double.parseDouble(job.getConfiguration().get("totalNum"));
                    sigma = deltaSum / totalNum;
                    System.out.println("deltaSum:" + deltaSum + "," + totalNum + "," + sigma);
                    ++i;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            Job job = new Job(conf, "JinYongsJiangHu_Job4_PageRank.PageRankIter");
            job.setJarByClass(PageRankIter.class);
            job.getConfiguration().set("deltaSum", String.valueOf(0));
            job.getConfiguration().set("totalNum", String.valueOf(0));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(PageRankIterMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(PageRankIterReducer.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + String.valueOf(i)));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            job.waitForCompletion(true);
            ++i;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    */

}
