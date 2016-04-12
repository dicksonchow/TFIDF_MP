package cloud.computing.tfidf;

import cloud.computing.tfidf.FirstJob.FirstMapper;
import cloud.computing.tfidf.FirstJob.FirstReducer;
import cloud.computing.tfidf.FourthJob.FourthMapper;
import cloud.computing.tfidf.SecondJob.SecondMapper;
import cloud.computing.tfidf.SecondJob.SecondReducer;
import cloud.computing.tfidf.ThirdJob.ThirdMapper;
import cloud.computing.tfidf.ThirdJob.ThirdReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
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
    static Configuration conf;

    public static void main(String[] args) throws Exception
    {
        if (args.length != 2){
            System.err.println("Usage: [input] [output]");
            System.exit(-1);
        }

        App app = new App();

        conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String input_dir = args[0];
        String output_dir = args[1];

        Path input = new Path(input_dir);
        ContentSummary cs = fs.getContentSummary(input);
        conf.setLong("total_no_of_docs", cs.getFileCount());

        fs.delete(new Path(output_dir + "_1"), true);
        app.executeFirstMapReduce(input_dir, output_dir + "_1");

        fs.delete(new Path(output_dir + "_2"), true);
        app.executeSecondMapReduce(output_dir + "_1", output_dir + "_2");

        fs.delete(new Path(output_dir + "_3"), true);
        app.executeThirdMapReduce(output_dir + "_2", output_dir + "_3");

        fs.delete(new Path(output_dir), true);
        app.executeFourthMapReduce(output_dir + "_3", output_dir);

        fs.delete(new Path(output_dir + "_1"), true);
        fs.delete(new Path(output_dir + "_2"), true);
        fs.delete(new Path(output_dir + "_3"), true);
    }

    public int executeFirstMapReduce(String input, String output) throws Exception {
        Job job = Job.getInstance(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(App.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int executeSecondMapReduce(String input, String output) throws Exception {
        Job job = Job.getInstance(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(App.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int executeThirdMapReduce(String input, String output) throws Exception {
        Job job = Job.getInstance(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ThirdMapper.class);
        job.setReducerClass(ThirdReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(App.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int executeFourthMapReduce(String input, String output) throws Exception {
        Job job = Job.getInstance(conf);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(FourthMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(App.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
