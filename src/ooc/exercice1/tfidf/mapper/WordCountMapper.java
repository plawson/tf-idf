package ooc.exercice1.tfidf.mapper;

import ooc.exercice1.tfidf.keys.WordDocIdWritableComparable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by plawson on 18/10/2017.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, WordDocIdWritableComparable, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private WordDocIdWritableComparable wordAndDoc = new WordDocIdWritableComparable();
    private Set<String> stopWords = new HashSet<String>();
    private FileSystem hdfs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        this.hdfs = FileSystem.get(context.getConfiguration());

        try {

            URI[] stopWordsCachedFiles = context.getCacheFiles();
            if (stopWordsCachedFiles != null && stopWordsCachedFiles.length > 0)
                readFile(stopWordsCachedFiles[0].getRawPath());


        } catch (IOException ex) {

            System.err.println("Error during mapper setup: " + ex.getMessage());
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().replaceAll("[0-9,;.:!?()]", "").replaceAll("[-'\"]", " ").toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();
            if (token.length() > 3) {

                if (!this.stopWords.contains(token)) {

                    InputSplit inputSplit = context.getInputSplit();
                    //FileSplit fileSplit = (FileSplit) inputSplit;

                    String str1 = inputSplit.toString().split(":")[2];
                    String[] str2 = str1.split("/");
                    String fileName = str2[str2.length - 1];


                    this.wordAndDoc.set(token, fileName);
                    context.write(this.wordAndDoc, this.one);
                }

            }
        }

    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        while(context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        cleanup(context);
    }

    private void readFile(String cachedFile) {

        try {

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.hdfs.open(new Path(cachedFile))));
            String stopWord = null;
            while ((stopWord = bufferedReader.readLine()) != null) {
                this.stopWords.add(stopWord);
            }

        } catch (IOException ex) {
            System.err.println("Error while reading stop words: " + ex.getMessage());
        }

    }
}
