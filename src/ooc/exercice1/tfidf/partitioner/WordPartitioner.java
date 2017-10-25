package ooc.exercice1.tfidf.partitioner;

import ooc.exercice1.tfidf.keys.JoinWordKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<JoinWordKey, Text> {

    @Override
    public int getPartition(JoinWordKey key, Text text, int numReduceTasks) {
        return (key.getJoinKey().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
