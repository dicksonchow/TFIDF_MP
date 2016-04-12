package cloud.computing.tfidf.ThirdJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class ThirdReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String[] temp;
        ArrayList<String> val = new ArrayList<String>();
        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            val.add(it.next().toString());
            count++;
        }

        for (String s: val) {
            // output (filename:word, count_of_a_word:total_word_count:count_word_in_doc)
            // temp[0] -> filename, temp[1] -> count_of_a_word, temp[2] -> total_word_count
            temp = s.split(":");
            context.write(new Text(temp[0] + ":" + key), new Text(temp[1] + ":" + temp[2] + ":" + count));
        }
    }
}
