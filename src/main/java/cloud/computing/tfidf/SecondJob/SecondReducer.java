package cloud.computing.tfidf.SecondJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class SecondReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // input format (filename, word:count)
        String[] str;
        int toBeAdd, count = 0;
        HashMap hm = new HashMap();
        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            str = it.next().toString().split(":");
            // str[0] -> word, str[1] -> count
            toBeAdd = Integer.parseInt(str[1]);
            hm.put(str[0], toBeAdd);
            count += toBeAdd;
        }

        Set set = hm.entrySet();
        Iterator i = set.iterator();

        while (i.hasNext()) {
            Map.Entry me = (Map.Entry) i.next();
            context.write(new Text(key + ":" + me.getKey()), new Text(me.getValue() + ":" + count));
        }
    }
}
