package ooc.exercice1.tfidf.driver;

import ooc.exercice1.tfidf.keys.WordDocIdWritableComparable;
import ooc.exercice1.tfidf.mapper.TFIDFMapper;
import ooc.exercice1.tfidf.mapper.TFIDFSortMapper;
import ooc.exercice1.tfidf.mapper.WordCountMapper;
import ooc.exercice1.tfidf.mapper.WordPerDocMapper;
import ooc.exercice1.tfidf.reducer.TFIDFReducer;
import ooc.exercice1.tfidf.reducer.TFIDFSortReducer;
import ooc.exercice1.tfidf.reducer.WordCountReducer;
import ooc.exercice1.tfidf.reducer.WordPerDocReducer;
import ooc.exercice1.tfidf.sort.TFIDFSortComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TFIDFDriver extends Configured implements Tool {

    private static final Path TFIDF_SORTED_20_OUTPUT = new Path("/tf-idf/sorted");

    @Override
    public int run(String[] args) throws Exception {

        // Check # of input parameters
        if (args.length != 6) {
            System.out.println("Usage: [stop words] [input_dir] [word_count_output_dir] [word_per_doc_output_dir] [tfidf_output_dir]");
            System.exit(-1);
        }

        Configuration conf = getConf();
        Path stopWordsFile = new Path(args[1]);
        Path inputDirectory = new Path(args[2]);
        Path wordCountDirectory = new Path(args[3]);
        Path wordPerDocDirectory = new Path(args[4]);
        Path tfidfDirectory = new Path(args[5]);

        // Count number of documents
        int numberOfDocuments = 0;
        FileSystem fileSystem = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(inputDirectory, false);
        while (remoteIterator.hasNext()) {
            numberOfDocuments++;
            remoteIterator.next();
        }

        if (numberOfDocuments == 0) {
            throw new Exception("No document found in " + inputDirectory);
        }

        conf.setInt("numberOfDocuments", numberOfDocuments);

        boolean succeed = wordCount(conf, stopWordsFile, inputDirectory, wordCountDirectory);

        if (!succeed) {
            throw new IllegalStateException("Word count failed!");
        }

        succeed = wordPerDoc(conf, wordCountDirectory, wordPerDocDirectory);
        if (!succeed) {
            throw new IllegalStateException("Word Per Doc failed!");
        }

        succeed = tfidf(conf, wordPerDocDirectory, tfidfDirectory);
        if (!succeed) {
            throw new IllegalStateException("TF-IDF computation failed !");
        }

        return sort(conf, tfidfDirectory);
    }

    private boolean wordCount(Configuration conf, Path stopWordsFile, Path inputDirectory, Path wordCountDirectory) throws IOException, InterruptedException, ClassNotFoundException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("WordCount");
        // Add Cache File
        job.addCacheFile(stopWordsFile.toUri());
        // Driver, Mapper and Reducer
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // Keys and Values
        job.setOutputKeyClass(WordDocIdWritableComparable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input and output
        FileInputFormat.addInputPath(job, inputDirectory);
        FileOutputFormat.setOutputPath(job, wordCountDirectory);
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if (fileSystem.exists(wordCountDirectory)) {
            fileSystem.delete(wordCountDirectory, true);
        }

        return job.waitForCompletion(true);
    }

    private boolean wordPerDoc(Configuration conf, Path wordCountDirectory, Path wordPerDocDirectory) throws IOException, InterruptedException, ClassNotFoundException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("WordPerDoc");
        // Driver, Mapper and Reducer
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(WordPerDocMapper.class);
        job.setReducerClass(WordPerDocReducer.class);
        // keys and values
        //job.setOutputKeyClass(WordDocIdWritableComparable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input and output
        FileInputFormat.addInputPath(job, wordCountDirectory);
        FileOutputFormat.setOutputPath(job, wordPerDocDirectory);
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if (fileSystem.exists(wordPerDocDirectory)) {
            fileSystem.delete(wordPerDocDirectory, true);
        }

        return job.waitForCompletion(true);
    }

    private boolean tfidf(Configuration conf, Path wordPerDocDirectory, Path tfidfDirectory) throws  IOException, InterruptedException, ClassNotFoundException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("TF-IDF");
        // Driver, Mapper, Reducer
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
        // keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input and output
        FileInputFormat.addInputPath(job, wordPerDocDirectory);
        FileOutputFormat.setOutputPath(job, tfidfDirectory);
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if (fileSystem.exists(tfidfDirectory)) {
            fileSystem.delete(tfidfDirectory, true);
        }

        return job.waitForCompletion(true);
    }

    private int sort(Configuration conf, Path inputSortDirectory) throws IOException, InterruptedException, ClassNotFoundException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("Sort TF-IDF Results");
        // Driver, Mapper, Reducer
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFSortMapper.class);
        job.setReducerClass(TFIDFSortReducer.class);
        // Keys, values
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Secondary sort order
        job.setSortComparatorClass(TFIDFSortComparator.class);
        // Input, output
        FileInputFormat.addInputPath(job, inputSortDirectory);
        FileOutputFormat.setOutputPath(job, TFIDF_SORTED_20_OUTPUT);
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if(fileSystem.exists(TFIDF_SORTED_20_OUTPUT)) {
            fileSystem.delete(TFIDF_SORTED_20_OUTPUT, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        TFIDFDriver driver = new TFIDFDriver();
        int res = ToolRunner.run(driver, args);
        System.exit(res);
    }
}
