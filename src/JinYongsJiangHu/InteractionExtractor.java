package JinYongsJiangHu;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Extract the interaction among characters in the novels.
 * Created by lubuntu on 16-7-7.
 */
public class InteractionExtractor {

    public static class InteractionExtractMapper extends Mapper<LongWritable, Text, Text, Text> {

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            String names = context.getConfiguration().get("names");
            String[] nameList = names.split(",");
            for(String name : nameList) {
                UserDefineLibrary.insertWord(name, "nr", 1000);
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text keyOut = new Text(fileName);
            StringBuilder strBuilder = new StringBuilder("");
            Result result = ToAnalysis.parse(value.toString());
            for(Term term : result) {
                if (term.getNatureStr().equals("nr"))
                    strBuilder.append(term.getName() + " ");
            }
            Text valueOut = new Text(strBuilder.toString().trim());
            context.write(keyOut, valueOut);
        }
    }

    public static class InteractionExtractReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("usage");
                System.exit(2);
            }
            Job job = new Job(conf, "JinYongsJiangHu_Job1_InteractionExtractor");
            job.setJarByClass(InteractionExtractor.class);
            job.getConfiguration().set("names", readNames(otherArgs[0]));
            job.setMapperClass(InteractionExtractMapper.class);
            job.setReducerClass(InteractionExtractReducer.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
            job.waitForCompletion(true);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Read from the file and get the name list string, separated with colon
     * @param fileName  the name of the people_name_list file
     * @return the string of names, seperated with ','
     */
    private static String readNames(String fileName) {
        StringBuilder names = new StringBuilder("");
        int num = 0;  // the number of names in the file
        File file = new File(fileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();
            names.append(line.trim());
            ++num;
            while((line = reader.readLine()) != null) {
                names.append(',' + line.trim());
                ++num;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("The number of names is " + num + ".");
        return names.toString();
    }
}
