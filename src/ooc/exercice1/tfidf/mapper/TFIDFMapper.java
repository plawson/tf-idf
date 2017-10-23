package ooc.exercice1.tfidf.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");
        context.write(new Text(values[0]), new Text(values[1] + "\t" + values[2] + "\t" + values[3]));
    }
}
