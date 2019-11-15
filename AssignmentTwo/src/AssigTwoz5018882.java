import com.google.common.collect.Lists;
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
    JavaPairRDD<String, Edge> step1 = input.mapToPair((PairFunction<String, String, Edge>) s -> {
      String[] parts = s.split(",");
      String n1 = parts[0];
      String n2 = parts[1];
      Integer distance = Integer.parseInt(parts[2]);
      return new Tuple2<>(n1, new Edge(n2, distance));
    });
    JavaPairRDD<String, Iterable<Edge>> step2 = step1.flatMapToPair(
      (PairFlatMapFunction<Tuple2<String, Edge>, String, Edge>) input12 -> {
        ArrayList<Tuple2<String, Edge>> ret = new ArrayList<>();
        String n1 = input12._1;
        String n2 = input12._2.n;
        Integer d = input12._2.d;
        ret.add(new Tuple2<>(n1, new Edge(n2, d)));
        ret.add(new Tuple2<>(n2, null));
        return ret.iterator();
      }).groupByKey();
    int numVertices = step2.groupByKey().collect().size();
    Map<String, Iterable<Edge>> graph = step1.groupByKey().collectAsMap();
    PairFlatMapFunction<Tuple2<String, Iterable<Edge>>, String, Tuple2<String, Edge>> f1 =
      (PairFlatMapFunction<Tuple2<String, Iterable<Edge>>, String, Tuple2<String, Edge>>) input1 -> {
        ArrayList<Tuple2<String, Tuple2<String, Edge>>> ret = new ArrayList<>();
        String n0 = input1._1;
        for (Edge e : input1._2) {
          if (e == null) {
            continue;
          }
          ret.add(new Tuple2<>(e.n, new Tuple2<>(String.format("%s-%s", n0, e.n), new Edge(e.n, e.d))));
        }
        return ret.iterator();
      };
    JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> step3 =
      step2.filter(
        (Function<Tuple2<String, Iterable<Edge>>, Boolean>) input15 ->
          input15._1.equals(startNode) ? true : false)
        .flatMapToPair(f1).groupByKey();
    PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Edge>>>, String, Tuple2<String, Edge>> f =
      (PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Edge>>>, String, Tuple2<String, Edge>>)
        input14 -> {
          ArrayList<Tuple2<String, Tuple2<String, Edge>>> ret = new ArrayList<>();
          for (Tuple2<String, Edge> t : input14._2) {
            String oldPath = t._1;
            Set<String> traversedNodes = new HashSet<>();
            String[] parts = oldPath.split("-");
            traversedNodes.addAll(Arrays.stream(parts).collect(Collectors.toList()));
            Edge e = t._2;
            if (e == null) {
              return ret.iterator();
            }
            Iterable<Edge> neighbours = graph.get(e.n);
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
              Tuple2<String, Edge> value = new Tuple2<>(
                newPath,
                new Edge(neighbour.n, e.d + neighbour.d)
              );
              ret.add(new Tuple2<>(neighbour.n, value));
            }
          }
          return ret.iterator();
        };
    JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> curr = step3;
    JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> acc = curr;
    for (int i = 0; i < numVertices; i++) {
      JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> temp = curr;
      curr = temp.flatMapToPair(f).groupByKey();
      acc = acc.union(curr);
    }

    JavaRDD<NodePathCost> result = acc.groupByKey()
      .map(
        (Function<Tuple2<String, Iterable<Iterable<Tuple2<String, Edge>>>>, NodePathCost>) input13 -> {
          Integer min = Integer.MAX_VALUE;
          String destination = input13._1;
          Tuple2<String, Edge> minPathEdge = null;
          for (Iterable<Tuple2<String, Edge>> it : input13._2) {
            for (Tuple2<String, Edge> t : it) {
              if (t._2.d < min) {
                min = t._2.d;
                minPathEdge = t;
              }
            }
          }
          return new NodePathCost(destination, minPathEdge._1, minPathEdge._2.d);
        });
//      .sortByKey().values().filter((Function<NodePathCost, Boolean>) nodePathCost -> nodePathCost.n.equals(startNode) ? false : true);
    result.collect().forEach(System.out::println);
    JavaPairRDD<String, NodePathCost> result2 = result.mapToPair(new PairFunction<NodePathCost, String, NodePathCost>() {
      @Override
      public Tuple2<String, NodePathCost> call(NodePathCost nodePathCost) throws Exception {
        return new Tuple2<>(nodePathCost.n, nodePathCost);
      }
    });
    result2.union(step2.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Edge>>, String>() {
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
