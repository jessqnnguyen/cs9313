import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * My solution to this problem is as follows:
 *  1) I create a map of the graph from the input mapping source nodes to half edges (Tuple2<String, HalfEdge>).
 *     To make sure I include nodes in the graph that don't have outgoing edges for
 *     every edge I output 2 tuples. For example: If edge = N0 -> N1 with weight 4 then I output:
 *     (N0 -> N1, 4) and (N1 -> null)
 *  2) Process the map created in step 1 to get only the start node's neighbours and then map to
 *     (String (Destination node) --> Iterable<NodePathCost>) i.e node path cost lists keyed by the destination node
 *     such that when you group this map by keys you obtain all the possible paths to this destination node.
 *  3) Storing an accumulative RDD (by unioning it with previous RDDs) loop through the graph NUM_VERTICES - 1 times
 *     to process neighbours for all nodes in the graph (except the start node)
 *  4) After we have processed all the neighbours for each node in the graph then for each destination node reduce it
 *     to 1 NodePathCost that is, take the NodePathCost with the minimum cost for that destination node.
 *  5) Create a graph of unreachable NodePathCost objects for all objects in the graph setting d = Integer.MAX_VALUE
 *     so that sorting by the distance value in ascending order later doesn't affect unreachable nodes.
 *  6) Union this graph of unreachable nodes with the final result from step 4) and delete all unreachable node entries
 *     from this union where there already exists a minimum NodePathCost object.
 *  7) Remap the final results' NodePathCosts where d = Integer.MAX_VALUE to d = -1.
 *  8) Collect output and save to supplied output directory.
 */
public class AssigTwoz5018882 {

  /**
   * Represents half an edge in the graph.
   * Given an edge N0 -> N1 with weight 4 then
   * this object would store n = N1 and d = 4.
   */
  public static class HalfEdge implements Serializable {
    /** Node (second half) of edge */
    String n;
    /** Distance */
    Integer d;

    public HalfEdge(String n, Integer d) {
      this.n = n;
      this.d = d;
    }

    @Override
    public String toString() {
      return String.format("(%s, %d)", this.n, this.d);
    }
  }

  /**
   * Represents a destination node path cost from the start node.
   */
  public static class NodePathCost implements Serializable {
    /** Destination node from start node */
    String n;
    /** String path of node */
    String path;
    /** Distance accrued on path so far from start node */
    Integer d;

    public NodePathCost(String n, String path, Integer d) {
      this.n = n;
      this.path = path;
      this.d = d;
    }

    @Override
    public String toString() {
      return String.format("%s,%d,%s", this.n, this.d, this.path);
    }
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
      .setAppName("Assignment Two")
      .setMaster("local");
    String startNode = args[0];
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> input = context.textFile(args[1]);

    // Map into edges keyed by the start connection node of the edge.
    // E.g. Given 1 edge 'N0,N1,4' --> Return 2 edges N0->N1 and N1-> null
    // The second edge is to make sure no node is missed even if it has
    // zero outgoing edges.
    JavaPairRDD<String, Iterable<HalfEdge>> step1 = input.flatMapToPair((PairFlatMapFunction<String, String, HalfEdge>) s -> {
      ArrayList<Tuple2<String, HalfEdge>> ret = new ArrayList<>();
      String[] parts = s.split(",");
      String n1 = parts[0];
      String n2 = parts[1];
      Integer distance = Integer.parseInt(parts[2]);
      ret.add(new Tuple2<>(n1, new HalfEdge(n2, distance)));
      ret.add(new Tuple2<>(n2, null));
      return ret.iterator();
    }).groupByKey();
    int numVertices = step1.collect().size();
    Map<String, Iterable<HalfEdge>> graph = step1.collectAsMap();

    // Filter out map constructed in step 1 to retrieve only the outgoing edges of the start node. Remap
    // it so that it maps a NodePathCost object to the node that the node path cost object is currently at i.e.
    // the destination node.
    PairFlatMapFunction<Tuple2<String,Iterable<HalfEdge>>, String, NodePathCost> processStartNodeNeighboursFunc =
      new PairFlatMapFunction<Tuple2<String,Iterable<HalfEdge>>, String, NodePathCost>() {
        @Override
        public Iterator<Tuple2<String, NodePathCost>> call(Tuple2<String, Iterable<HalfEdge>> input) throws Exception {
          ArrayList<Tuple2<String, NodePathCost>> ret = new ArrayList<>();
          String n1 = input._1;
          for (HalfEdge e : input._2) {
            if (e == null) {
              continue;
            }
            String n2 = e.n;
            Integer d = e.d;
            String path = String.format("%s-%s", n1, n2);
            ret.add(new Tuple2<>(n2, new NodePathCost(n2, path, d)));
          }
          return ret.iterator();
        }
      };
    JavaPairRDD<String, Iterable<NodePathCost>> startNodeConnections =
      step1.filter((Function<Tuple2<String, Iterable<HalfEdge>>, Boolean>) input15 ->
          input15._1.equals(startNode) ? true : false)
        .flatMapToPair(processStartNodeNeighboursFunc).groupByKey();

    PairFlatMapFunction<Tuple2<String,Iterable<NodePathCost>>, String, NodePathCost> processNodeNeighboursFunc =
      new PairFlatMapFunction<Tuple2<String,Iterable<NodePathCost>>, String, NodePathCost>() {
      @Override
      public Iterator<Tuple2<String, NodePathCost>> call(Tuple2<String, Iterable<NodePathCost>> input) throws Exception {
        ArrayList<Tuple2<String, NodePathCost>> ret = new ArrayList<>();
        for (NodePathCost npc: input._2) {
          String oldPath = npc.path;
          Set<String> traversedNodes = new HashSet<>();
          String[] parts = oldPath.split("-");
          traversedNodes.addAll(Arrays.stream(parts).collect(Collectors.toList()));
          String n = npc.n;
          Integer d = npc.d;
          Iterable<HalfEdge> neighbours = graph.get(n);
          if (neighbours == null) {
            return ret.iterator();
          }
          for (HalfEdge neighbour : neighbours) {
            if (neighbour == null) {
              continue;
            }
            // Check cycle
            if (traversedNodes.contains(neighbour.n)) {
              continue;
            }
            String path = String.format("-%s", neighbour.n);
            String newPath = oldPath.concat(path);
            NodePathCost value = new NodePathCost(neighbour.n, newPath, d + neighbour.d);
            ret.add(new Tuple2<>(neighbour.n, value));
          }
        }
        return ret.iterator();
      }
    };

    // Now that the start node has been processed, from there iterate for a max of numVertices - 1 (since the
    // start node has already been processed) to check all nodes that are reachable from the start node
    // and obtain a map of NodePathCost objects keyed by nodes at the end of the path, grouping the last nodes in the
    // path together so we can obtain the minimum path in the next step.
    JavaPairRDD<String, Iterable<NodePathCost>> curr = startNodeConnections;
    JavaPairRDD<String, Iterable<NodePathCost>> acc = curr;
    for (int i = 0; i < numVertices - 1; i++) {
      JavaPairRDD<String, Iterable<NodePathCost>> temp = curr;
      curr = temp.flatMapToPair(processNodeNeighboursFunc).groupByKey();
      acc = acc.union(curr);
    }

    // Map the final result of {EndNode, Iterable<NodePathCost>} to {EndNode, NodePathCost} where the resulting
    // value is the minimum NodePathCost to the EndNode in the list.
    JavaPairRDD<String, NodePathCost> reachableNodesWithMinNodePathCosts = acc.groupByKey()
      .mapToPair(
        (PairFunction<Tuple2<String, Iterable<Iterable<NodePathCost>>>, String, NodePathCost>) input13 -> {
          Integer min = Integer.MAX_VALUE;
          String destination = input13._1;
          NodePathCost minNodePathCost = null;
          for (Iterable<NodePathCost> it : input13._2) {
            for (NodePathCost npc : it) {
              if (npc.d < min) {
                min = npc.d;
                minNodePathCost = npc;
              }
            }
          }
          return new Tuple2<>(destination, new NodePathCost(destination, minNodePathCost.path, minNodePathCost.d));
        });

    // Mark all nodes in the graph to be unreachable.
    JavaPairRDD<String, NodePathCost> unreachableNodes =
      step1.flatMap(new FlatMapFunction<Tuple2<String, Iterable<HalfEdge>>, String>() {
        @Override
        public Iterator<String> call(Tuple2<String, Iterable<HalfEdge>> input) throws Exception {
          List<String> ret = new ArrayList<>();
          if (!input._1.equals(startNode)) {
            ret.add(input._1);
          }
          for (HalfEdge e : input._2) {
            if (e == null) {
              continue;
            } else if (e.n.equals(startNode)) {
              continue;
            }
            ret.add(e.n);
          }
          return ret.iterator();
        }
      }).mapToPair(new PairFunction<String, String, NodePathCost>() {
      @Override
      public Tuple2<String, NodePathCost> call(String s) throws Exception {
        return new Tuple2<>(s, new NodePathCost(s, "", Integer.MAX_VALUE));
      }
    });

    // Union the map of unreachable nodes (all nodes in the graph) with the reachable nodes and ignore the NodePathCost
    // objects that are being used to mark a node as unreachable if there exists another path that isn't marked
    // as unreachable. Then sort by keys so we obtain the final output ordered by min path costs.
    // Then set unreachable nodes distances from Integer.MAX_VALUE to -1 after this sort.
    JavaPairRDD<String, Iterable<NodePathCost>> combined = reachableNodesWithMinNodePathCosts.union(unreachableNodes)
      .groupByKey();
    combined.mapToPair(new PairFunction<Tuple2<String, Iterable<NodePathCost>>, Integer, NodePathCost>() {
        @Override
        public Tuple2<Integer, NodePathCost> call(Tuple2<String, Iterable<NodePathCost>> input) throws Exception {
          NodePathCost output = null;
          for (NodePathCost n : input._2) {
            if (output == null) {
              output = n;
            }
            if (n.d == Integer.MAX_VALUE) {
              continue;
            }
          }
          return new Tuple2<>(output.d, output);
        }
      }).sortByKey().values().map(new Function<NodePathCost, NodePathCost>() {
      @Override
      public NodePathCost call(NodePathCost nodePathCost) throws Exception {
        if (nodePathCost.d == Integer.MAX_VALUE) {
          nodePathCost.d = -1;
        }
        return nodePathCost;
      }
    }).coalesce(1).saveAsTextFile(args[2]);
  }
}
