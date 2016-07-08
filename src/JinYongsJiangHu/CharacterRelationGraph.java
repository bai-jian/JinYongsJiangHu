package JinYongsJiangHu;


import org.apache.commons.math3.util.Pair;
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
import java.util.ArrayList;
import java.util.Iterator;


/** Construct the Character Relation Graph
 * Created by lubuntu on 16-7-8.
 */
public class CharacterRelationGraph {

    static class CharacterRelationGraphMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Text keyOut = new Text();
            Text valueOut = new Text();
            String[] names = key.toString().split(",");
            if (names.length != 2) {
                System.err.println("The key from map of CharacterRelationGraph contains " + names.length + " names.");
                System.exit(3);
            }
            keyOut.set(names[0]);
            valueOut.set(names[1] + "," + value.toString());
            context.write(keyOut, valueOut);
            keyOut.set(names[1]);
            valueOut.set(names[0] + "," + value.toString());
            context.write(keyOut, valueOut);
        }
    }

    static class CharacterRelationGraphReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Pair<String, Integer>> list = new ArrayList();
            for(Text value : values) {
                String[] key_value = value.toString().split(",");
                list.add(new Pair(key_value[0], new Integer(key_value[1])));
            }
            double sum = 0;
            Iterator<Pair<String, Integer>> iter = list.iterator();
            while(iter.hasNext()) {
                sum += iter.next().getSecond();
            }
            StringBuilder strBuilder = new StringBuilder("");
            iter = list.iterator();
            if (iter.hasNext()) {
                Pair<String, Integer> pair = iter.next();
                String name = pair.getFirst();
                double freq = pair.getSecond() / sum;
                strBuilder.append(name + ":" + freq);
            }
            while(iter.hasNext()) {
                Pair<String, Integer> pair = iter.next();
                String name = pair.getFirst();
                double freq = pair.getSecond() / sum;
                strBuilder.append('|' + name + ':' + freq);
            }
            Text valueOut = new Text(strBuilder.toString());
            context.write(key, valueOut);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("usage: CharacterRelationGraph");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job3_CharacterRelationGraph");
            job.setJarByClass(CharacterRelationGraph.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(CharacterRelationGraphMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(CharacterRelationGraphReducer.class);
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
}
