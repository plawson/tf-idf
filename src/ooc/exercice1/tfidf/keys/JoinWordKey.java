package ooc.exercice1.tfidf.keys;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinWordKey implements WritableComparable<JoinWordKey> {

    private String joinKey; // word
    private int sourceIndex; // 1 = frequencyincorpus, 2 = wordperdoc

    public JoinWordKey() {

    }

    public JoinWordKey(String joinKey, int sourceIndex) {
        this.joinKey = joinKey;
        this.sourceIndex = sourceIndex;
    }

    @Override
    public int compareTo(JoinWordKey o) {

        int res = this.joinKey.compareTo(o.joinKey);
        if (res == 0) {
            res = Double.compare(this.sourceIndex, o.sourceIndex);
        }
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        WritableUtils.writeString(out, this.joinKey);
        WritableUtils.writeVInt(out, this.sourceIndex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.joinKey = WritableUtils.readString(in);
        this.sourceIndex = WritableUtils.readVInt(in);
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public int getSourceIndex() {
        return sourceIndex;
    }

    public void setSourceIndex(int sourceIndex) {
        this.sourceIndex = sourceIndex;
    }

    @Override
    public String toString() {
        return this.joinKey + "\t" + this.sourceIndex;
    }
}
