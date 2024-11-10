import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCount implements WritableComparable<SumCount> {
    private IntWritable count;
    private Text genre;

    public SumCount() {
        set(new IntWritable(0), "");
    }

    public SumCount(IntWritable count, String genre) {
        set(count, genre);
    }

    public void set(IntWritable count, String genre) {
        this.count = count;
        this.genre = new Text(genre);
    }

    public IntWritable getCount() {
        return count;
    }

    public String getGenre() {
        return genre.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        count.write(out);
        genre.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count.readFields(in);
        genre.readFields(in);
    }

    @Override
    public int compareTo(SumCount o) {
        return count.compareTo(o.getCount());
    }
}
