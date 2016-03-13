package cloud.computing.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class TFIDFReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable total_count = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int word_count = 0;
        Iterator<IntWritable> it = values.iterator();

        while (it.hasNext())
            word_count += it.next().get();

        total_count.set(word_count);
        context.write(key, total_count);
    }
}
