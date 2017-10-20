package ooc.exercice1.tfidf.reducer;

import ooc.exercice1.tfidf.keys.WordDocIdWritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by plawson on 18/10/2017.
 */
public class WordCountReducer extends Reducer<WordDocIdWritableComparable, IntWritable, WordDocIdWritableComparable, IntWritable> {

    private IntWritable totalWordCount = new IntWritable();

    @Override
    public void reduce(final WordDocIdWritableComparable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }
        this.totalWordCount.set(sum);
        context.write(key, totalWordCount);
    }
}
