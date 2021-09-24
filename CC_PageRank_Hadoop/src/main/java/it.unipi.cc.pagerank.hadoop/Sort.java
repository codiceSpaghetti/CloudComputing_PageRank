package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.serialize.Node;
import it.unipi.cc.pagerank.hadoop.serialize.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sort {
    private static final String OUTPUT_PATH = "/sort";
    private String output;

    private static Sort instance = null;  // Singleton

    private Sort() { }

    public static Sort getInstance()
    {
        if (instance == null)
            instance = new Sort();

        return instance;
    }

    public String getOutputPath() { return output; }


    public static class SortMapper extends Mapper<Text, Text, Page, NullWritable> {
        private static final Node node = new Node();
        private static final Page reducerKey = new Page();
        private static final NullWritable nullValue = NullWritable.get();

        // For each node, create a Page object and emit it
        @Override
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            node.setFromJson(value.toString());
            reducerKey.set(key.toString(), node.getPageRank());
            context.write(reducerKey, nullValue);
        }
    }


    public static class SortReducer extends Reducer<Page, NullWritable, Text, DoubleWritable> {
        private static final Text title = new Text();
        private static final DoubleWritable rank = new DoubleWritable();

        // Emit the already sorted list of pages (exploits of Shuffle & Sort phase)
        @Override
        public void reduce(final Page key, final Iterable<NullWritable> values, final Context context) throws IOException, InterruptedException {
            title.set(key.getTitle());
            rank.set(key.getRank());
            context.write(title, rank);
        }
    }


    public boolean run(final String input, final String baseOutput) throws Exception {
        this.output = baseOutput + OUTPUT_PATH;

        // set configuration
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        // set mapper/reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Page.class);
        job.setMapOutputValueClass(NullWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    /*
    public static void main(final String[] args) throws Exception {
        // set configuration
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        // set mapper/reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Page.class);
        job.setMapOutputValueClass(NullWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
     */
}
