package MapReduce.LabelPropagation;


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
import java.util.HashMap;
import java.util.Map;


/**
 * Created by lubuntu on 16-7-12.
 */
public class LabelPropagationIter {

    static class LabelPropagationIterMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Text keyOut = new Text();
            Text valueOut = new Text();
            String[] lists = value.toString().split(",");
            String label = lists[0];
            for(int i = 1; i < lists.length; ++i) {
                keyOut.set(lists[i]);
                valueOut.set("src," + key.toString() + "," + label);
                context.write(keyOut, valueOut);
            }
            int firstCommaIndex = value.toString().indexOf(',');
            valueOut.set(value.toString().substring(firstCommaIndex + 1));
            context.write(key, valueOut);

        }
    }

    static class LabelPropagationIterReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>();
            String list = null;
            for(Text value: values) {
                String[] lists = value.toString().split(",");
                if (lists[0].equals("src")) {
                    String k = lists[2];
                    Integer v = map.get(k);
                    map.put(k, v == null ? 1 : v + 1);
                } else {
                    list = value.toString();
                }
            }
            Map.Entry<String, Integer> max = null;
            for (Map.Entry<String, Integer> e : map.entrySet()) {
                if (max == null || e.getValue() > max.getValue())
                    max = e;
            }
            Text valueOut = new Text(max.getKey() + "," + list);
            context.write(key, valueOut);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("usage: LabelPropagation.LabelPropagationIter <in> <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job5_LabelPropagation.LabelPropagationIter");
            job.setJarByClass(LabelPropagationIter.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(LabelPropagationIterMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(LabelPropagationIterReducer.class);
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

}
