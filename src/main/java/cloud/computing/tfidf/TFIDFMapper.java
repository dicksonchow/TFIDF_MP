package cloud.computing.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private String filename = "";
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String str[] = value.toString().toLowerCase().replaceAll("[^a-z ]", "") .split("\\s+");

        for (String s: str){
            word.set(filename + ':' + s);
            context.write(word, one);
        }
    }
}
