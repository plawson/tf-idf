package ooc.exercice1.tfidf.group;

import ooc.exercice1.tfidf.keys.JoinWordKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinKeyGroup extends WritableComparator {

    public JoinKeyGroup() {
        super(JoinWordKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        JoinWordKey k1 = (JoinWordKey)a;
        JoinWordKey k2 = (JoinWordKey)b;

        return k1.getJoinKey().compareTo(k2.getJoinKey());
    }
}
