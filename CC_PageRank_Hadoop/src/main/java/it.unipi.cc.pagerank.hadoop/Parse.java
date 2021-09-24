package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.parser.Parser;
import it.unipi.cc.pagerank.hadoop.parser.ParserWikiMicro;
import it.unipi.cc.pagerank.hadoop.serialize.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Parse {
    private static final String OUTPUT_PATH = "/parse";
    private String output;

    private static Parse instance = null;  // Singleton

    private Parse() { }

    public static Parse getInstance()
    {
        if (instance == null)
            instance = new Parse();

        return instance;
    }

    public String getOutputPath() { return output; }


    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text reducerKey = new Text();
        private static final Text reducerValue = new Text();
        private static final Text empty = new Text("");
        private static final Parser wiki_microParser = new ParserWikiMicro();

        private static String record;
        private static String title;
        private static List<String> outLinks;

        // For each line of the input (web page/node), emit title and out-links (if any)
        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            record = value.toString();
            wiki_microParser.setStringToParse(record);
            title = wiki_microParser.getTitle();
            outLinks = wiki_microParser.getOutLinks();

            if(title != null) {
                reducerKey.set(title);

                if(outLinks.size() > 0) {
                    for (String outLink : outLinks) {
                        reducerValue.set(outLink);
                        context.write(reducerKey, reducerValue);
                        context.write(reducerValue, empty); // emit out-link (useful to keep track of pages not present as XML)
                    }
                } else
                    context.write(reducerKey, empty); // dangling node
            }
        }
    }


    public static class ParseCombiner extends Reducer<Text, Text, Text, Text> {
        private static String value;
        private static boolean emptyFound;

        // For each key, emitt all it's not-empty values + 0 or 1 empty values
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            emptyFound = false;
            for(Text outValue: values) {
                value = outValue.toString();
                if(!value.equals(""))
                    context.write(key, outValue);
                else {
                    if(!emptyFound) {
                        context.write(key, outValue);
                        emptyFound = true;
                    }
                }
            }
        }
    }


    public static class ParseReducer extends Reducer<Text, Text, Text, Node> {
        private int pageCount;
        private static final Node outValue = new Node();

        private static List<String> adjacencyList;
        private static String value;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.pageCount = context.getConfiguration().getInt("page.count", 0);
        }

        // For each page, emit the title and its node features
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            adjacencyList = new LinkedList<>();
            for(Text outLink: values) {
                value = outLink.toString();
                if(!value.equals(""))
                    adjacencyList.add(value);
            }
            outValue.setAdjacencyList(adjacencyList);
            outValue.setPageRank(1.0d/this.pageCount);
            outValue.setIsNode(true);
            context.write(key, outValue);
        }
    }


    public boolean run(final String input,
                       final String baseOutput,
                       final int numReducers,
                       final int pageCount) throws Exception {
        this.output = baseOutput + OUTPUT_PATH;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(Parse.class);

        // set mapper/combiner/reducer
        job.setMapperClass(ParseMapper.class);
        job.setCombinerClass(ParseCombiner.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set page.count for initializing the ranks
        job.getConfiguration().setInt("page.count", pageCount);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    /*
    public static void main(final String[] args) throws Exception {
        // set configurations
        final Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.output.textoutputformat.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(Parse.class);

        // set mapper/reducer
        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set page.count for initializing the ranks
        int pageCount = Count.getInstance().getPageCount(otherArgs[0], otherArgs[1]);
        job.getConfiguration().setInt("page.count", pageCount);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
     */
}
