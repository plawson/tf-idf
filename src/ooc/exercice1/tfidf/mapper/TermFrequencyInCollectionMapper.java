package ooc.exercice1.tfidf.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TermFrequencyInCollectionMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] values = line.toString().split("\t");
        context.write(new Text(values[0]), new Text(new StringBuilder(values[1]).append("\t").append(values[2]).append("\t").append(values[3]).toString()));
    }
}
