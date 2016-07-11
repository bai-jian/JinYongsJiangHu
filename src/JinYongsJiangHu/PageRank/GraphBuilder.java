package JinYongsJiangHu.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lubuntu on 16-7-11.
 */
public class GraphBuilder {

    static class GraphBuildMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder strBuilder = new StringBuilder("1.0");
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
            if (args.length < 2) {
                System.err.println("usage: GraphBuilder <in> <out>");
                System.exit(0);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job4_PageRank.GraphBuilder");
            job.setJarByClass(GraphBuilder.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(GraphBuildMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
