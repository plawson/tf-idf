package ooc.exercice1.tfidf.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

/**
 * Created by plawson on 24/10/2017.
 */
public class TFIDFSortReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    private static final DecimalFormat decimalFormat = new DecimalFormat("###.######");

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> valuesIterator = values.iterator();
        for (int i = 0; valuesIterator.hasNext() && i < 20; i++) {

            context.write(new Text(decimalFormat.format(key.get())), new Text(valuesIterator.next()));
        }
    }
}
