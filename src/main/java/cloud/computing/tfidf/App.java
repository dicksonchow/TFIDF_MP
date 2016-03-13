package cloud.computing.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;

public class App
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2){
            System.err.println("Usage: [input] [output]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path input_dir = new Path(args[0]);
        Path output_dir = new Path(args[1]);

        if (fs.exists(output_dir))
            fs.delete(output_dir, true);

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input_dir);
        FileOutputFormat.setOutputPath(job, output_dir);

        job.setJarByClass(App.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
