package cloud.computing.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str[] = value.toString().split("\\s+");

        for (String s: str){
            word.set(s);
            context.write(word, one);
        }
    }
}
