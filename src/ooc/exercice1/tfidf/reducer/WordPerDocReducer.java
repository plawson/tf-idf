package ooc.exercice1.tfidf.reducer;

import ooc.exercice1.tfidf.keys.WordDocIdWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordPerDocReducer extends Reducer<Text, Text, WordDocIdWritableComparable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String, String> wordCount = new HashMap<>();
        int wordPerDoc = 0;

        for (Text value: values) {
            String[] currentWordCount = value.toString().split("\t");
            wordCount.put(currentWordCount[0], currentWordCount[1]);
            wordPerDoc += Integer.parseInt(currentWordCount[1]);
        }

        for(String word: wordCount.keySet()) {
            context.write(new WordDocIdWritableComparable(word, key.toString()), new Text(wordCount.get(word) + "\t" + wordPerDoc));
        }
    }
}
