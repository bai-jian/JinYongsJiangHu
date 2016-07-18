package MapReduce.LabelPropagation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by lubuntu on 16-7-12.
 */
public class GraphBuilder {

    static class GraphBuilderMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder strBuilder = new StringBuilder(key.toString());
            String[] pairs = value.toString().split(",");
            for(String pair : pairs) {
                strBuilder.append("," + pair.split(":")[0]);
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
                System.err.println("usage: LabelPropagation.GraphBuilder <in> <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job5_LabelPropagation.GraphBuilder");
            job.setJarByClass(GraphBuilder.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(GraphBuilderMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
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
