package ooc.exercice1.tfidf.mapper;

import ooc.exercice1.tfidf.keys.JoinWordKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TFIDFJoinMapper extends Mapper<LongWritable, Text, JoinWordKey, Text> {

    private int sourceIndex = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        String path = ((FileSplit)context.getInputSplit()).getPath().toString();
        if (path.contains(context.getConfiguration().get("frequencyincollection"))) {
            this.sourceIndex = 1;
        } else {
            this.sourceIndex = 2;
        }
    }

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] values = line.toString().split("\t");
        StringBuilder sb = new StringBuilder();
        for (int i=1; i < values.length; i++) {
            sb.append(values[i]).append("\t");
        }
        sb.setLength(sb.length() - 1);

        context.write(new JoinWordKey(values[0], this.sourceIndex), new Text(sb.toString()));

    }
}
