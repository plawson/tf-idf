package ooc.exercice1.tfidf.reducer;

import ooc.exercice1.tfidf.keys.JoinWordKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

public class TFIDFJoinReducer extends Reducer<JoinWordKey, Text, Text, Text> {

    private static final DecimalFormat decimalFormat = new DecimalFormat("###.######");

    @Override
    public void reduce(JoinWordKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int numberOfDocuments = context.getConfiguration().getInt("numberOfDocuments", 0);
        Iterator<Text> iterator = values.iterator();

        // First key is the frequency in collection
        int termFrequencyInCollection = Integer.parseInt(iterator.next().toString());
        // Process the (docId wordcount wordperdoc)
        String[] strValues;
        while (iterator.hasNext()) {
            strValues = iterator.next().toString().split("\t");
            double tfidf = Integer.parseInt(strValues[1]) * Integer.parseInt(strValues[2]) * Math.log10(numberOfDocuments/termFrequencyInCollection);
            context.write(new Text(key.getJoinKey() + "\t" + strValues[0]), new Text(decimalFormat.format(tfidf)));
        }
    }
}
