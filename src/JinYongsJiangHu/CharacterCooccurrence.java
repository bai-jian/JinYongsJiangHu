package JinYongsJiangHu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by lubuntu on 16-7-8.
 */
public class CharacterCooccurrence {

    class CharacterCooccurrenceMapper extends Mapper<Text, Text, Text, IntWritable> {

        private IntWritable one = new IntWritable(1);

        public void map(Text key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            String[] names = value.toString().split(",");
            for(int i = 0; i < names.length; ++i) {
                for(int j = 0; j < names.length; ++j) {
                    Text keyOut = new Text(names[i] + "," + names[j]);
                    context.write(keyOut, one);
                }
            }
        }
    }

    class CharacterCooccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            IntWritable valueOut = new IntWritable(sum);
            context.write(key, valueOut);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length <= 2) {
                System.err.println("usage: CharacterCooccurrence ");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job2_CharacterCooccurrence");
            job.setJarByClass(CharacterCooccurrence.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(CharacterCooccurrenceMapper.class);
            // job.setMapOutputKeyClass(Text.class);
            // job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(CharacterCooccurrenceReducer.class);
            // job.setOutputFormatClass(TextOutputFormat.class)
            job.setOutputKeyClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
