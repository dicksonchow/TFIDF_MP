package cloud.computing.tfidf.ThirdJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ThirdMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input (filename:word, count_of_a_word:total_word_count)
        String[] str = value.toString().split(":|\\s+");

        // str[0] -> filename, str[1] -> word, str[2] -> count_of_a_word, str[3] -> total_word_count
        context.write(new Text(str[1]), new Text(str[0] + ":" + str[2] + ":" + str[3]));
    }
}
