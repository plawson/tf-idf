package ooc.exercice1.tfidf.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

    private static final DecimalFormat decimalFormat = new DecimalFormat("###.#########");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numberOfDocuments = context.getConfiguration().getInt("numberOfDocuments", 0);
        int termFrequencyInCollection = 0;
        Map<String, String> docIdAndCounts = new HashMap<>();

        for (Text value: values) {
            String[] currentDocIdAndCounts = value.toString().split("\t");
            docIdAndCounts.put(currentDocIdAndCounts[0], currentDocIdAndCounts[1] + "\t" + currentDocIdAndCounts[2]);
            termFrequencyInCollection++;
        }

        for (String docId: docIdAndCounts.keySet()) {
            String[] wordCountAndWordPerDoc = docIdAndCounts.get(docId).split("\t");
            double tfidf = (Double.parseDouble(wordCountAndWordPerDoc[0]) / Double.parseDouble(wordCountAndWordPerDoc[1])) * Math.log10(numberOfDocuments/termFrequencyInCollection);
            context.write(new Text(key + "\t" + docId), new Text(decimalFormat.format(tfidf)));
        }

    }
}
