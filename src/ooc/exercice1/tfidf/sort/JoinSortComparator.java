package ooc.exercice1.tfidf.sort;

import ooc.exercice1.tfidf.keys.JoinWordKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinSortComparator extends WritableComparator {

    public JoinSortComparator() {
        super(JoinWordKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        JoinWordKey k1 = (JoinWordKey)a;
        JoinWordKey k2 = (JoinWordKey)b;

        int res = k1.getJoinKey().compareTo(k1.getJoinKey());
        if (res == 0) {
            res = Double.compare(k1.getSourceIndex(), k2.getSourceIndex());
        }
        return res;
    }
}
