import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class AssigTwoz5018882 {

  public static class Edge implements Serializable {
    String n;
    Integer d;

    public Edge(String n, Integer d) {
      this.n = n;
      this.d = d;
    }

    @Override
    public String toString() {
      return String.format("(%s, %d)", this.n, this.d);
    }
  }

  public static class NodePathCost implements Serializable {
    String n;
    String path;
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

    // Step 1: Map into into edges keyed by the start connection node of the edge.
    // E.g. Given 1 edge 'N0,N1,4' --> Return 2 edges N0->N1 and N1-> null
    // The second edge is to make sure no node is missed even if it has
    // zero outgoing connections.
    JavaPairRDD<String, Iterable<Edge>> step1 = input.flatMapToPair((PairFlatMapFunction<String, String, Edge>) s -> {
      ArrayList<Tuple2<String, Edge>> ret = new ArrayList<>();
      String[] parts = s.split(",");
      String n1 = parts[0];
      String n2 = parts[1];
      Integer distance = Integer.parseInt(parts[2]);
      ret.add(new Tuple2<>(n1, new Edge(n2, distance)));
      ret.add(new Tuple2<>(n2, null));
      return ret.iterator();
    }).groupByKey();
    int numVertices = step1.collect().size();
    Map<String, Iterable<Edge>> graph = step1.collectAsMap();

    // Step 2: Filter out map constructed in step 1 to retrieve only the outgoing edges of the start node. Remap
    // it so that it maps the node path and
    PairFlatMapFunction<Tuple2<String,Iterable<Edge>>, String, NodePathCost> processStartNodeNeighboursFunc =
      new PairFlatMapFunction<Tuple2<String,Iterable<Edge>>, String, NodePathCost>() {
        @Override
        public Iterator<Tuple2<String, NodePathCost>> call(Tuple2<String, Iterable<Edge>> input) throws Exception {
          ArrayList<Tuple2<String, NodePathCost>> ret = new ArrayList<>();
          String n1 = input._1;
          for (Edge e : input._2) {
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
      step1.filter((Function<Tuple2<String, Iterable<Edge>>, Boolean>) input15 ->
          input15._1.equals(startNode) ? true : false)
        .flatMapToPair(processStartNodeNeighboursFunc).groupByKey();

    JavaPairRDD<String, Iterable<NodePathCost>> curr = startNodeConnections;
    JavaPairRDD<String, Iterable<NodePathCost>> acc = curr;
    for (int i = 0; i < numVertices; i++) {
      JavaPairRDD<String, Iterable<NodePathCost>> temp = curr;
      curr = temp.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<NodePathCost>>, String, NodePathCost>() {
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
            Iterable<Edge> neighbours = graph.get(n);
            if (neighbours == null) {
              return ret.iterator();
            }
            for (Edge neighbour : neighbours) {
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
      }).groupByKey();
      acc = acc.union(curr);
    }

    JavaRDD<NodePathCost> result = acc.groupByKey()
      .map(
        (Function<Tuple2<String, Iterable<Iterable<NodePathCost>>>, NodePathCost>) input13 -> {
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
          return new NodePathCost(destination, minNodePathCost.path, minNodePathCost.d);
        });

    JavaPairRDD<String, NodePathCost> result2 = result.mapToPair(new PairFunction<NodePathCost, String, NodePathCost>() {
      @Override
      public Tuple2<String, NodePathCost> call(NodePathCost nodePathCost) throws Exception {
        return new Tuple2<>(nodePathCost.n, nodePathCost);
      }
    });
    result2.union(step1.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Edge>>, String>() {
      @Override
      public Iterator<String> call(Tuple2<String, Iterable<Edge>> input) throws Exception {
        ArrayList<String> ret = new ArrayList<>();
        ret.add(input._1);
        for (Edge e : input._2) {
          if (e == null) {
            continue;
          }
          ret.add(e.n);
        }
        return ret.iterator();
      }
    }).filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        return s.equals(startNode) ? false : true;
      }
    }).mapToPair(new PairFunction<String, String, NodePathCost>() {
      @Override
      public Tuple2<String, NodePathCost> call(String s) throws Exception {
        return new Tuple2<>(s, new NodePathCost(s, "", Integer.MAX_VALUE));
      }
    })).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<NodePathCost>>, Integer, NodePathCost>() {
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
