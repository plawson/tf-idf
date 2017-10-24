package ooc.exercice1.tfidf.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TermFrequencyInCollectionReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int termFrequencyInCollection = 0;

        for (Text value: values) {

            termFrequencyInCollection++;
        }

        context.write(key, new IntWritable(termFrequencyInCollection));
    }
}
