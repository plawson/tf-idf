package ooc.exercice1.tfidf.driver;

import ooc.exercice1.tfidf.group.JoinKeyGroup;
import ooc.exercice1.tfidf.keys.JoinWordKey;
import ooc.exercice1.tfidf.keys.WordDocIdWritableComparable;
import ooc.exercice1.tfidf.mapper.*;
import ooc.exercice1.tfidf.partitioner.WordPartitioner;
import ooc.exercice1.tfidf.reducer.*;
import ooc.exercice1.tfidf.sort.JoinSortComparator;
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
    private  static final String FREQUENCY_IN_COLLECTION_DIRECTORY_STR = "/tf-idf/frequencyincollection";
    private  static final Path FREQUENCY_IN_COLLECTION_DIRECTORY = new Path(FREQUENCY_IN_COLLECTION_DIRECTORY_STR);

    private boolean isJoinNeeded = false;

    @Override
    public int run(String[] args) throws Exception {

        // Check # of input parameters
        if (args.length != 6 && args.length != 7) {
            System.out.println("Usage: <stop words> <input_dir> <word_count_output_dir> <word_per_doc_output_dir> <tfidf_output_dir> [number_of_documents]");
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
        if ( args.length == 7) {

            numberOfDocuments = Integer.parseInt(args[6]);
            this.isJoinNeeded = true;

        } else {

            FileSystem fileSystem = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(inputDirectory, false);
            while (remoteIterator.hasNext()) {
                numberOfDocuments++;
                remoteIterator.next();
            }
        }

        if (numberOfDocuments == 0) {
            throw new Exception("No document found in " + inputDirectory);
        }

        conf.setInt("numberOfDocuments", numberOfDocuments);
        conf.set("frequencyincollection", FREQUENCY_IN_COLLECTION_DIRECTORY_STR);


        boolean succeed = wordCount(conf, stopWordsFile, inputDirectory, wordCountDirectory);

        if (!succeed) {
            throw new IllegalStateException("Word count failed!");
        }

        succeed = wordPerDoc(conf, wordCountDirectory, wordPerDocDirectory);
        if (!succeed) {
            throw new IllegalStateException("Word Per Doc failed!");
        }

        if (this.isJoinNeeded) {

            succeed = frequencyInCollection(conf, wordPerDocDirectory);
            if (!succeed) {
                throw new IllegalStateException("Frequency in collection failed!");
            }

            succeed = joiningTfidf(conf, wordPerDocDirectory, tfidfDirectory);
            if (!succeed) {
                throw new IllegalStateException("Joining TF-IDF computation failed !");
            }

        } else {

            succeed = tfidf(conf, wordPerDocDirectory, tfidfDirectory);
            if (!succeed) {
                throw new IllegalStateException("TF-IDF computation failed !");
            }
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

    private boolean frequencyInCollection(Configuration conf, Path wordPerDocDirectory) throws IOException, InterruptedException, ClassNotFoundException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("Frequency in Collection");
        // Driver, Mapper, Reducer
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TermFrequencyInCollectionMapper.class);
        job.setReducerClass(TermFrequencyInCollectionReducer.class);
        // Keys, values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input, Output
        FileInputFormat.addInputPath(job, wordPerDocDirectory);
        FileOutputFormat.setOutputPath(job, FREQUENCY_IN_COLLECTION_DIRECTORY);
        FileSystem fileSystem = FileSystem.newInstance(conf);
        if (fileSystem.exists(FREQUENCY_IN_COLLECTION_DIRECTORY)) {
            fileSystem.delete(FREQUENCY_IN_COLLECTION_DIRECTORY, true);
        }

        return job.waitForCompletion(true);
    }

    private boolean joiningTfidf(Configuration conf, Path wordPerDocDirectory, Path tfidfDirectory) throws IOException, InterruptedException, ClassNotFoundException {

        // Job creation
        Job job = Job.getInstance(conf);
        job.setJobName("Join TF-IDF");
        // Number of reducer tasks
        job.setNumReduceTasks(3); // This is an arbitrary value. It should normally depends on the total number of documents.
        // Driver, Mapper, Reducer
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFJoinMapper.class);
        job.setReducerClass(TFIDFJoinReducer.class);
        // Partitioner, group, secondary sort
        job.setPartitionerClass(WordPartitioner.class);
        job.setGroupingComparatorClass(JoinKeyGroup.class);
        job.setSortComparatorClass(JoinSortComparator.class);
        // Keys, values
        job.setOutputKeyClass(JoinWordKey.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input, output
        FileInputFormat.addInputPath(job, FREQUENCY_IN_COLLECTION_DIRECTORY);
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
