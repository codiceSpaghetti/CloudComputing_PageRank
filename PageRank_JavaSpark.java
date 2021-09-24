package it.unipi.spark;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class CC_PageRank_JavaSpark {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: PageRank <input> <base output> <# of iterations> <random jump probability>");
            System.exit(1);
        }
        final String INPUT_FILE = args[0];
        final String DATA_SINK = args[1]; // Output file
        final int ITERATION = Integer.parseInt(args[2]);
        final float ALPHA = Float.parseFloat(args[3]);

        final SparkConf sparkConf = new SparkConf().setMaster("yarn").setAppName("PageRank");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);


        final JavaRDD<String> lines = sc.textFile(INPUT_FILE).cache();

        // ================== COUNT PHASE ======================

        final long page_count = lines.count();

        // ================== PARSE PHASE ======================

        //  --------------------------- Identification of "nodes_outlinks" ----------------------------
        // Return an RDD of key-value pairs that corresponds to
        // all the possible nodes (initial nodes + all possible url linked by them, even replicated) with the corresponding
        // sequence of outlinks (in case of dangling nodes -e.g. url linked by nodes- the outlinks list is empty)
        JavaPairRDD<String, Iterable<String>> all_nodes_outlinks = lines.flatMapToPair(new PairFlatMapFunction<String, String, Iterable<String>>() {
            @Override
            public Iterator<Tuple2<String, Iterable<String>>> call(String line) throws Exception {
                // extracting node-outlinks described in the input file
                String node = line.substring(line.indexOf("<title>") + 7, line.indexOf("</title>"));
                String regex = "\\[\\[(.*?)\\]\\]";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(line);
                List<String> outlinks = new ArrayList<String>();
                while (matcher.find()) {
                    outlinks.add(line.substring(matcher.start() + 2, matcher.end() - 2));
                }

                List<String> emptyList = new ArrayList<String>();
                List<Tuple2<String, Iterable<String>>> myTupleList = new ArrayList<>();
                myTupleList.add(new Tuple2<String, Iterable<String>>(node, outlinks));

                // extracting urls in each outlinks list
                if(!outlinks.isEmpty()){
                    for (String url : outlinks) {
                        myTupleList.add(new Tuple2<String, Iterable<String>>(url, emptyList));
                    }
                }
                return myTupleList.iterator();
            }
        });

        // Since elements can be replicated in "all_nodes_outlinks" rdd, we merge outlinks of each node,
        // returning a JavaPairRDD with only distinct nodes and their corresponding outlinks list
        JavaPairRDD<String, Iterable<String>> allDistinct_nodes_outlinks = all_nodes_outlinks.reduceByKey(new Function2<Iterable<String>, Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> outlinks1, Iterable<String> outlinks2) throws Exception {
                List<String> all_outlinks = new ArrayList<String>();
                for (String next : outlinks1) {
                    all_outlinks.add(next);
                }
                for (String next : outlinks2) {
                    all_outlinks.add(next);
                }
                return all_outlinks;
            }
        }).cache();

        // .keys(): Return an RDD with only the lists of all possible distinct nodes -keys- (nodes + outlinks)
        // without the correspondant outlinks list -values-).
        // It is performed separately since it is needed for two subsequent operations.
        JavaRDD<String> allDistinct_nodes = allDistinct_nodes_outlinks.keys().cache();

        //  --------------------------- initializing Mass ----------------------------
        // Return an RDD of key-value pairs that corresponds to
        // all distinct nodes + their initial mass
        JavaPairRDD<String, Double> nodes_mass = allDistinct_nodes.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String node) throws Exception {
                return new Tuple2<String, Double>(node, 1.0 / page_count);
            }
        });

        //  --------------------------- "nodes_outlinks_mass" mass fan-out: (A) handling NODES ----------------------------
        // Return an RDD of key-value pairs that corresponds to
        // all distinct nodes + 0 mass
        // Operation needed to realize the fan-out of mass from nodes to their links.
        // Performed only once (not in the iterations phase) since its result is the same for every iteration and
        // can be computed directly from "allDistinct_node" RDD.
        JavaPairRDD<String, Double> node_0mass = allDistinct_nodes.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String node) throws Exception {
                return new Tuple2<String, Double>(node, (Double) 0.0);
            }
        }).cache();


        // ================== RANKING PHASE until convergence (iterative) ======================
        for(int it=0; it<ITERATION; it++){

            //  --------------------------- association node-mass ----------------------------
            // Associate every node-oulinks pair with its mass (it changes at the end of every iteration).
            JavaPairRDD <String, Tuple2<Iterable<String>, Double>> nodes_outlinks_mass = allDistinct_nodes_outlinks.join(nodes_mass);





            //  --------------------------- "nodes_outlinks_mass" mass fan-out: (B) handling LINKS ----------------------------
            // We perform the fan-out of the mass toward each link.

            // Return an RDD with only the tuples <outlinks, nodeMass> -values of "nodes_outlinks_mass" RDD-.
            //          nodeMass = mass of the node (previous iteration) that correspond to that list of outlinks
            JavaRDD<Tuple2<Iterable<String>, Double>> outlinks_nodeMass = nodes_outlinks_mass.values();

            // Return an RDD with a tuple for each outlink with the corresponding weighted mass that
            // the corresponding node is trasmitting to the outlink: It results in an RDD of <outlink, mass>
            JavaPairRDD<String, Double> outlink_linkMass = outlinks_nodeMass.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> myTuple) throws Exception {
                    List<Tuple2<String, Double>> myTupleList = new ArrayList<>();
                    List<String> outlinksList = new ArrayList<String>();
                    outlinksList.addAll((Collection<? extends String>) myTuple._1());  // ._1() get first element of input tuple: the Iterable<String> of outlinks
                    Double node_Mass = myTuple._2();
                    if (outlinksList.isEmpty()) {      //handling dangling nodes
                        return myTupleList.iterator();
                    } else {
                        int cardinality_outlinks = outlinksList.size();
                        for (String url : outlinksList) {
                            myTupleList.add(new Tuple2<String, Double>(url, node_Mass / cardinality_outlinks));
                        }
                    }
                    return myTupleList.iterator();
                }
            });

            //  --------------------------- "nodes_outlinks_mass" mass fan-out: MERGING (A) and (B) results ----------------------------
            //  Return an RDD with all the possible outlinks <outlinks, nodeMass> -values of "nodes_outlinks_mass" RDD-.
            //          nodeMass = mass of the node (previous iteration) that correspond to that list of outlinks
            JavaPairRDD<String, Double> all_nodes_mass = outlink_linkMass.union(node_0mass);

            //  --------------------------- Computing nodes' mass fan-in ----------------------------
            // Return an RDD with all distinct nodes and their corresponding rank: <node, rank>
            // where rank is the sum of all weighted mass given by outlinks of other nodes (it is 0 for dangling nodes)
            JavaPairRDD<String, Double> nodes_rank = all_nodes_mass.reduceByKey(new Function2<Double, Double, Double> () {
                @Override
                public Double call(Double mass1, Double mass2){
                    return mass1 + mass2;
                }
            });

            //  --------------------------- Computing nodes' new mass/rank ----------------------------
            // Return an RDD with all distinct nodes and their corresponding rank,
            // where rank is the computation
            nodes_mass = nodes_rank.mapValues(new Function<Double, Double> () {
                @Override
                public Double call(Double nodeMassSum) throws Exception{
                    return ALPHA/page_count + (1-ALPHA)* nodeMassSum;
                }
            });
        }

        // ================== SORTING PHASE ======================
        JavaPairRDD<String, Double> sorted_nodes_mass = nodes_mass.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
        sorted_nodes_mass.saveAsTextFile(DATA_SINK);
    }
}