package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.serialize.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Rank {
    private static final String OUTPUT_PATH = "/rank";
    private String output;

    private static Rank instance = null;  // Singleton

    private Rank() { }

    public static Rank getInstance()
    {
        if (instance == null)
            instance = new Rank();

        return instance;
    }

    public String getOutputPath() { return output; }


    public static class RankMapper extends Mapper<Text, Text, Text, Node> {
        private static final Text reducerKey = new Text();
        private static final Node reducerValue = new Node();
        private static final List<String> empty = new LinkedList<>();
        
        private static List<String> outLinks;
        private static double mass;

        // For each line of the input (page title and its node features)
        // (1) emit page title and its node features to maintain the graph structure
        // (2) emit out-link pages with their mass (rank share)
        @Override
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            reducerKey.set(key.toString());
            reducerValue.setFromJson(value.toString());
            context.write(reducerKey, reducerValue); // (1)

            outLinks = reducerValue.getAdjacencyList();
            mass = reducerValue.getPageRank() / outLinks.size();

            reducerValue.setAdjacencyList(empty);
            reducerValue.setIsNode(false);
            for(String outLink: outLinks) {
                reducerKey.set(outLink);
                reducerValue.setPageRank(mass);
                context.write(reducerKey, reducerValue); // (2)
            }
        }
    }


    public static class RankCombiner extends Reducer<Text, Node, Text, Node> {
        private static final Node outValue = new Node();
        private static final List<String> empty = new LinkedList<>();
        
        private static double aggregatedRank;

        // For each key 
        // (1) pass along the graph-structure
        // (2) sum up the entrant rank values and emit the aggregate
        @Override
        public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
            aggregatedRank = 0.0;
            outValue.setAdjacencyList(empty);
            outValue.setIsNode(false);

            for(Node p: values) {
                if(p.isNode())
                    context.write(key, p); // (1)
                else
                    aggregatedRank += p.getPageRank();
            }
            outValue.setPageRank(aggregatedRank);
            context.write(key, outValue); // (2)
        }
    }


    public static class RankReducer extends Reducer<Text, Node, Text, Node>{
        private double alpha;
        private int pageCount;
        private static final Node outValue = new Node();
        private static final List<String> empty = new LinkedList<>();
        
        private static double rank;
        private static double newPageRank;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.alpha = context.getConfiguration().getDouble("alpha", 0);
            this.pageCount = context.getConfiguration().getInt("page.count", 0);
        }

        // For each node associated to a page
        // (1) if it is a complete node, recover the graph structure from it
        // (2) else, get from it an incoming rank contribution
        @Override
        public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
            rank = 0.0;
            outValue.setAdjacencyList(empty);
            outValue.setIsNode(false);

            for(Node p: values) {
                if(p.isNode())
                    outValue.set(p);  // (1)
                else
                    rank += p.getPageRank(); // (2)
            }
            newPageRank = (this.alpha / ((double)this.pageCount)) + ((1 - this.alpha) * rank);
            outValue.setPageRank(newPageRank);
            context.write(key, outValue);
        }
    }


    public boolean run(final String input,
                       final String baseOutput,
                       final double alpha,
                       final int numReducers,
                       final int pageCount,
                       final int iteration) throws Exception {
        this.output = baseOutput + OUTPUT_PATH + "-" + iteration;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Rank-" + iteration);
        job.setJarByClass(Rank.class);

        // set mapper/combiner/reducer
        job.setMapperClass(RankMapper.class);
        job.setCombinerClass(RankCombiner.class);
        job.setReducerClass(RankReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set the random jump probability alpha and the page count
        job.getConfiguration().setDouble("alpha", alpha);
        job.getConfiguration().setInt("page.count", pageCount);

        // set number of reducer tasks to be used
        job.setNumReduceTasks(numReducers);

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
        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Rank");
        job.setJarByClass(Rank.class);

        // set mapper/reducer
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

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
