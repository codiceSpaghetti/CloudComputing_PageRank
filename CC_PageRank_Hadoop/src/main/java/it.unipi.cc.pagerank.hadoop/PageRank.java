package it.unipi.cc.pagerank.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5) {
            System.err.println("Usage: PageRank <input> <base output> <# of iterations> <# of reducers> <random jump probability>");
            System.exit(1);
        }
        final String INPUT = otherArgs[0];
        final String BASE_OUTPUT = otherArgs[1];
        final int ITERATIONS = Integer.parseInt(otherArgs[2]);
        final int REDUCERS = Integer.parseInt(otherArgs[3]);
        final double ALPHA = Double.parseDouble(otherArgs[4]);

        // Count Stage
        final int pageCount = Count.getInstance().getPageCount(INPUT, BASE_OUTPUT);
        if(pageCount < 1) {
            throw new Exception("Count job failed");
        }
        System.out.println(">> Count Stage completed");

        // Parse Stage
        if(!Parse.getInstance().run(INPUT, BASE_OUTPUT, REDUCERS, pageCount)) {
            throw new Exception("Parse job failed");
        }
        System.out.println(">> Parse Stage completed");

        // Rank Stage until convergence
        String nextInput = Parse.getInstance().getOutputPath();
        for(int i = 0; i < ITERATIONS; i++) {
            if(!Rank.getInstance().run(nextInput, BASE_OUTPUT, ALPHA, REDUCERS, pageCount, i))
                throw new Exception("Rank " + i + "-th job failed");
            nextInput = Rank.getInstance().getOutputPath();
            System.out.println(">> Iteration " + (i+1) + " completed");
        }
        System.out.println(">> Rank Stage completed");

        // Sort Stage
        if(!Sort.getInstance().run(nextInput, BASE_OUTPUT))
            throw new Exception("Sort job failed");
        System.out.println(">> Sort Stage completed");
    }
}
