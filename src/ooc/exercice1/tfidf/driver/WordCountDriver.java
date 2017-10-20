package ooc.exercice1.tfidf.driver;

import ooc.exercice1.tfidf.keys.WordDocIdWritableComparable;
import ooc.exercice1.tfidf.mapper.WordCountMapper;
import ooc.exercice1.tfidf.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by plawson on 18/10/2017.
 */
public class WordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 5) {
            System.out.println("args# " + args.length);
            for (String arg: args) {
                System.out.println(arg);
            }
            System.out.println("Usage: [stop words] [input1] [input2] [output]");
            System.exit(-1);
        }

        // Job creation
        Job job = Job.getInstance(getConf());
        job.setJobName("wordcount");
        // Driver, Map and Reduce
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        // Keys and values types
        job.setOutputKeyClass(WordDocIdWritableComparable.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Add cache file
        job.addCacheFile(new Path(args[1]).toUri());
        // Input
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class);
        // Output
        Path outputFilePath = new Path(args[4]);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        WordCountDriver wordCountDriver = new WordCountDriver();
        int res = ToolRunner.run(wordCountDriver, args);
        System.exit(res);
    }
}
