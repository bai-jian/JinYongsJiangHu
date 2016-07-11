package JinYongsJiangHu.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by lubuntu on 16-7-11.
 */
public class PageRankViewer {

    static class MyDoubleWritable extends DoubleWritable {
        @Override
        public int compareTo(DoubleWritable o) {
            return -super.compareTo(o);
        }
    }

    static class PageRankViewerMapper extends Mapper<Text, Text, MyDoubleWritable, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            double pr = Double.parseDouble(value.toString().split(",")[0]);
            MyDoubleWritable keyOut = new MyDoubleWritable();
            keyOut.set(pr);
            Text valueOut = key;
            context.write(keyOut, valueOut);
        }
    }



    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("usage: PageRankViewer <in> <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job4_PageRank.PageRankViewer");
            job.setJarByClass(PageRankViewer.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(PageRankViewerMapper.class);
            job.setMapOutputKeyClass(MyDoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
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
