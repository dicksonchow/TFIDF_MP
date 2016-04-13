package cloud.computing.tfidf.FourthJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FourthMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private long D_val = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        D_val = context.getConfiguration().getLong("total_no_of_docs", 0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input (filename:word, count_of_a_word:total_word_count:count_word_in_doc))
        // str[0] -> filename, str[1] -> word, str[2] -> count_of_a_word, str[3] -> total_word_count
        // str[4] -> count_word_in_doc
        String[] str = value.toString().split(":|\\s+");

        double WCInADoc = Double.parseDouble(str[2]);
        double totalWCOfDoc = Double.parseDouble(str[3]);
        double shownInDoc = Double.parseDouble(str[4]);

        double tfidfVal = (WCInADoc / totalWCOfDoc) * Math.log(D_val / shownInDoc);

        context.write(new Text(str[0]), new Text(str[1] + ":" + tfidfVal));
    }
}
