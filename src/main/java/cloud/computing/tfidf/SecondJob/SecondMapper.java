package cloud.computing.tfidf.SecondJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SecondMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input format (filename:word, count)
        // the things before "," is the key, those after "," is the value

        String[] str = value.toString().split(":|\\s+");

        // str[0] -> filename, str[1] -> word, str[2] -> count number
        context.write(new Text(str[0]), new Text(str[1] + ":" + str[2]));
    }
}
