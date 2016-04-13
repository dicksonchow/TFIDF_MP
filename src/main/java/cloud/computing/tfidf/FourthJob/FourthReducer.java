package cloud.computing.tfidf.FourthJob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class FourthReducer extends Reducer<Text, Text, Text, NullWritable>
{
    JSONObject json = new JSONObject();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] str = {};
        JSONObject nestedJson = new JSONObject();
        JSONArray jarr = new JSONArray();
        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            // (filename, word:tfidf)
            // str[0] -> word, str[1] -> tfidf
            Arrays.fill(str, null);
            str = it.next().toString().split(":");
            nestedJson.put(str[0], str[1]);
        }

        jarr.add(nestedJson);
        json.put(key.toString(), jarr);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        Object toBePrinted = mapper.readValue(json.toJSONString(), Object.class);
        context.write(new Text(mapper.defaultPrettyPrintingWriter().writeValueAsString(toBePrinted)), null);
    }
}
