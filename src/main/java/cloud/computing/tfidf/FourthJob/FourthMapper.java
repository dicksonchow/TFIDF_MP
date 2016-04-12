package cloud.computing.tfidf.FourthJob;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FourthMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
    private long D_val = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        D_val = context.getConfiguration().getLong("total_no_of_docs", 0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input (filename:word, count_of_a_word:total_word_count:count_word_in_doc))
        String[] keyVal = value.toString().split("\\s+");
        String[] str = keyVal[1].split(":");
        double WCInADoc = Double.parseDouble(str[0]);
        double totalWCOfDoc = Double.parseDouble(str[1]);
        double shownInDoc = Double.parseDouble(str[2]);

        double tfidfVal = (WCInADoc / totalWCOfDoc) * Math.log(D_val / shownInDoc);

        context.write(new Text(D_val + ":" + keyVal[0]), new DoubleWritable(tfidfVal));
    }
}
