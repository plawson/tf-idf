package ooc.exercice1.tfidf.keys;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by plawson on 18/10/2017.
 */
public class WordDocIdWritableComparable implements WritableComparable<WordDocIdWritableComparable> {

    private String word;
    private String docId;

    public WordDocIdWritableComparable() {

    }

    public  WordDocIdWritableComparable(String word, String docId) {
        this.word = word;
        this.docId = docId;
    }

    @Override
    public int compareTo(WordDocIdWritableComparable o) {

        if (o == null)
            return 0;

        int cnt = this.word.compareTo(o.word);
        return cnt == 0 ? this.docId.compareTo(o.docId) : cnt;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        //out.writeUTF(word);
        //out.writeUTF(docId);
        WritableUtils.writeString(out, this.word);
        WritableUtils.writeString(out, this.docId);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        //this.word = in.readUTF();
        //this.docId = in.readUTF();
        this.word = WritableUtils.readString(in);
        this.docId = WritableUtils.readString(in);

    }

    public void set(String word, String docId) {
        this.word = word;
        this.docId = docId;
    }

    public String toString() {
        return this.word + "\t" + this.docId;
    }

    public int hashCode() {
        String concat = this.word + this.docId;
        return concat.hashCode();
    }
}
