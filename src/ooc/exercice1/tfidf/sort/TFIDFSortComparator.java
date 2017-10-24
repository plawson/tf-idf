package ooc.exercice1.tfidf.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by plawson on 24/10/2017.
 */
public class TFIDFSortComparator extends WritableComparator {

    public TFIDFSortComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        DoubleWritable vala = (DoubleWritable)a;
        DoubleWritable valb = (DoubleWritable)b;

        return -(vala.compareTo(valb));
    }
}
