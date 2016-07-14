package JinYongsJiangHu.LabelPropagation;

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
import java.util.Iterator;

/**
 * Created by lubuntu on 16-7-12.
 */
public class LabelPropagationViewer {

    static class LabelPropagationViewerMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String label = value.toString().split(",")[0];
            Text keyOut = new Text(label);
            Text valueOut = key;
            context.write(keyOut, valueOut);
        }
    }

    static class LabelPropagationViewerReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder strBuilder = new StringBuilder("");
            Iterator iter = values.iterator();
            if (iter.hasNext()) {
                strBuilder.append(iter.next().toString());
            }
            while(iter.hasNext()) {
                strBuilder.append(',' + iter.next().toString());
            }
            Text valueOut = new Text(strBuilder.toString());
            context.write(key, valueOut);
        }
    }

    /*
    // The below mapper and reducer is to prepare the files to visualize the community graph
    static class LabelPropagationViewerMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Text keyOut = new Text();
            Text valueOut = new Text();
            // nodes
            String[] list = value.toString().split(",");
            String community = list[0];
            int size = list.length - 1;
            keyOut.set("node\t" + key);
            valueOut.set(key.toString() + "\t" + community + "\t" + size);
            context.write(keyOut, valueOut);
            // edges
            for(int i = 1; i < list.length; ++i) {
                if (key.toString().compareTo(list[i]) < 0)
                    keyOut.set("edge\t" + key.toString() + "\t" + list[i]);
                else
                    keyOut.set("edge\t" + list[i] + "\t" + key.toString());
                valueOut.set("Undirected\t" + community);
                context.write(keyOut, valueOut);
            }
        }
    }
    static class LabelPropagationViewerReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }
    */

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("usage: LabelPropagation.LabelPropagationViewer <in> <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job5_LabelPropagation.LabelPropagationViewer");
            job.setJarByClass(LabelPropagationViewer.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(LabelPropagationViewerMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(LabelPropagationViewerReducer.class);
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
