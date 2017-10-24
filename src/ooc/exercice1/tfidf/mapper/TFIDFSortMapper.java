package ooc.exercice1.tfidf.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by plawson on 24/10/2017.
 */
public class TFIDFSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] values = line.toString().split("\t");
        context.write(new DoubleWritable(Double.parseDouble(values[2])), new Text(values[0] + "\t" + values[1]));

    }
}
