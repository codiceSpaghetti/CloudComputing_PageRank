import sys
from operator import add
import re
from pyspark import SparkContext


def get_node_outlinks(line):
    node = line[line.find("<title>") + 7: line.find("</title>")]
    list_outlinks = re.findall("\\[\\[(.*?)\\]\\]", line)
    return node, list_outlinks


def distribute_mass(node, outlinks, mass):
    cardinality_outlinks = len(outlinks)
    for url in outlinks:
        yield url, mass/cardinality_outlinks
    yield node, 0    #otherwise i lose nodes that have no inlinks


def emit_node(node, outlinks):
    yield node, outlinks
    for url in outlinks:
        yield url, []


def extend(a, b):
    a.extend(b)
    return a


def compute_rank(summatory):
    return alpha/page_count + (1-alpha) * summatory


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: PageRank <input> <base output> <# of iterations> <random jump probability>", file=sys.stderr)
        sys.exit(-1)

    iteration = int(sys.argv[3])
    alpha = float(sys.argv[4])
    data_source = sys.argv[1]
    data_sink = sys.argv[2]

    sc = SparkContext("yarn", "PageRank")
    source_file = sc.textFile(data_source).cache()
    page_count = source_file.count()

    node_outlinks = source_file.map(get_node_outlinks) #for each node in the input file, get a tuple with the node
                                                         #and its adjency list in the form (node, [outlinks])

    all_nodes_outlinks = node_outlinks.flatMap(lambda n_o: emit_node(n_o[0], n_o[1])).reduceByKey(extend).cache() #for each node of the graph, get a tuple with the node
                                                                                                                  #and its adjency list in the form(node, [outlinks])

    mass = all_nodes_outlinks.map(lambda x: (x[0], 1 / page_count)) #for all the nodes in the graph get a tuple with the node and its mass with the initial value (node, mass)

    for iteration in range(iteration):
        node_outlinks_mass = all_nodes_outlinks.join(mass)  #for all node in the class get a tuple with the node and a tuple with its adjency list
                                                            #and its mass in the form(node, ([outlinks], mass))

        ranks = node_outlinks_mass.flatMap(lambda n_o_m: distribute_mass(n_o_m[0], n_o_m[1][0], n_o_m[1][1])) #for each contribution get a tuple with the node
                                                                                                            # and the contribution in the form (node, contribution)

        node_ranks = ranks.reduceByKey(add) #for each node get a tuple with the node and the sum of its contribution in the form (node, ranks)

        mass = node_ranks.mapValues(compute_rank) #for each node get a tuple with the node and its actual rank in the form (node, rank)

    sorted_mass = mass.sortBy(lambda x: -x[1])
    sorted_mass.saveAsTextFile(data_sink)

