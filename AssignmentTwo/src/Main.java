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

public class Main {

    static String minKey(List<String> vertices, Map<String, Integer> minDistancesToEdge, Set<String> mstSet) {
        // Initialize min value
        int min = Integer.MAX_VALUE;
        String minVertex = "";

        for (String v : vertices)
            if (!mstSet.contains(v) && minDistancesToEdge.get(v) < min) {
                min = minDistancesToEdge.get(v);
                minVertex = v;
            }

        return minVertex;
    }

    public static class ConnectedNode implements Serializable {
        String n1;
        String n2;
        Integer d;

        public ConnectedNode(String n1, String n2, Integer d) {
            this.n1 = n1;
            this.n2 = n2;
            this.d = d;
        }

        @Override
        public String toString() {
            return String.format("(%s, %s, %d)", this.n1, this.n2, this.d);
        }
    }

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

    public static Edge getEdge(String n1, String n2, Map<String, Iterable<Edge>> graph) {
        if (graph.get(n1) != null) {
            for (Edge e : graph.get(n1)) {
                if (e.n.equals(n2)) {
                    return e;
                }
            }
        }
        return null;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Assignment Two")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> input = context.textFile("input.txt");
        // start node, (end node, distance)
        JavaPairRDD<String, Edge> step1 = input.mapToPair((PairFunction<String, String, Edge>) s -> {
            String[] parts = s.split(",");
            String n1 = parts[0];
            String n2 = parts[1];
            Integer distance = Integer.parseInt(parts[2]);
            return new Tuple2<>(n1, new Edge(n2, distance));
        });
        JavaPairRDD<String, Iterable<Edge>> step2 = step1.flatMapToPair((PairFlatMapFunction<Tuple2<String, Edge>, String, Edge>) input12 -> {
            ArrayList<Tuple2<String, Edge>> ret = new ArrayList<>();
            String n1 = input12._1;
            String n2 = input12._2.n;
            Integer d = input12._2.d;
            ret.add(new Tuple2<>(n1, new Edge(n2, d)));
            ret.add(new Tuple2<>(n2, null));
            return ret.iterator();
        }).groupByKey();
        String startNode = step1.keys().collect().get(0);
        System.out.println("start node is " + startNode);
        Map<String, Iterable<Edge>> graph  = step1.groupByKey().collectAsMap();
        PairFlatMapFunction<Tuple2<String,Iterable<Edge>>, String, Tuple2<String, Edge>> f1 = (PairFlatMapFunction<Tuple2<String, Iterable<Edge>>, String, Tuple2<String, Edge>>) input1 -> {
            ArrayList<Tuple2<String, Tuple2<String, Edge>>> ret = new ArrayList<>();
            String n0 = input1._1;
            for (Edge e: input1._2) {
                if (e == null) {
                    continue;
                }
                ret.add(new Tuple2<>(e.n, new Tuple2<>(String.format("%s-%s", n0, e.n), new Edge(e.n,e.d))));
            }
            return ret.iterator();
        };
        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> step3 =
                step2.filter(
                        (Function<Tuple2<String, Iterable<Edge>>, Boolean>) input15 ->
                                input15._1.equals(startNode) ? true : false)
                        .flatMapToPair(f1).groupByKey();
        PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String, Edge>>>, String, Tuple2<String,Edge>> f  =
                new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Edge>>>, String, Tuple2<String, Edge>>() {
                    @Override
                    public Iterator<Tuple2<String, Tuple2<String, Edge>>> call(Tuple2<String, Iterable<Tuple2<String, Edge>>> input) throws Exception {
                        ArrayList<Tuple2<String, Tuple2<String,Edge>>> ret = new ArrayList<>();
                        for (Tuple2<String, Edge> t: input._2) {
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
                    }
                };
        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> curr = step3;
        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> acc = curr;
        acc.collect().forEach(System.out::println);
        for (int i =0; i < 6; i++) {
            JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> temp = curr;
            curr = temp.flatMapToPair(f).groupByKey();
            acc = acc.union(curr);
            acc.collect().forEach(System.out::println);
        }

        JavaRDD<NodePathCost> result = acc.groupByKey().mapToPair((PairFunction<Tuple2<String, Iterable<Iterable<Tuple2<String, Edge>>>>, Integer, NodePathCost>) input13 -> {
            Integer min = Integer.MAX_VALUE;
            String destination = input13._1;
            Tuple2<String, Edge> minPathEdge = null;
            for (Iterable<Tuple2<String, Edge>> it : input13._2) {
                if (Lists.newArrayList(it).isEmpty()) {
                    return new Tuple2<>(-1, new NodePathCost(destination, "", -1));
                }
                for (Tuple2<String, Edge> t : it) {
                    if (t._2.d < min) {
                        min = t._2.d;
                        minPathEdge = t;
                    }
                }
            }
            return new Tuple2<>(minPathEdge._2.d, new NodePathCost(destination, minPathEdge._1, minPathEdge._2.d));
        }).sortByKey().values().filter((Function<NodePathCost, Boolean>) nodePathCost -> nodePathCost.n.equals(startNode) ? false : true);
        result.collect().forEach(System.out::println);
        JavaPairRDD<String, NodePathCost> result2 = result.mapToPair(new PairFunction<NodePathCost, String, NodePathCost>() {
            @Override
            public Tuple2<String, NodePathCost> call(NodePathCost nodePathCost) throws Exception {
                return new Tuple2<>(nodePathCost.n, nodePathCost);
            }
        });
        result2.union(step2.flatMapToPair(f1).keys().filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.equals(startNode) ? false : true;
            }
        }).mapToPair(new PairFunction<String, String, NodePathCost>() {
            @Override
            public Tuple2<String, NodePathCost> call(String s) throws Exception {
                return new Tuple2<>(s, new NodePathCost(s, "", -1));
            }
        })).groupByKey().map(new Function<Tuple2<String,Iterable<NodePathCost>>, NodePathCost>() {
            @Override
            public NodePathCost call(Tuple2<String, Iterable<NodePathCost>> input) throws Exception {
                NodePathCost output = null;
                for (NodePathCost n : input._2) {
                    if (output == null) {
                        output = n;
                    }
                    if (n.d == -1) {
                        continue;
                    }
                }
                return output;
            }
        }).mapToPair(new PairFunction<NodePathCost, Integer, NodePathCost>() {
            @Override
            public Tuple2<Integer, NodePathCost> call(NodePathCost nodePathCost) throws Exception {
                return new Tuple2<>(nodePathCost.d, nodePathCost);
            }
        }).sortByKey().values().collect().forEach(System.out::println);


//        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> step4 = step3.flatMapToPair(f).groupByKey();
//        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> step5 = step4.flatMapToPair(f).groupByKey();
//        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> step6 = step5.flatMapToPair(f).groupByKey();
//        JavaPairRDD<String, Iterable<Tuple2<String, Edge>>> step7 = step6.flatMapToPair(f).groupByKey();
//
//        step3.union(step4).union(step5).union(step6).union(step7).groupByKey().mapToPair((PairFunction<Tuple2<String, Iterable<Iterable<Tuple2<String, Edge>>>>, Integer, NodePathCost>) input13 -> {
//            Integer min = Integer.MAX_VALUE;
//            String destination = input13._1;
//            Tuple2<String, Edge> minPathEdge = null;
//            for (Iterable<Tuple2<String, Edge>> it : input13._2) {
//                for (Tuple2<String, Edge> t : it) {
//                    if (t._2.d < min) {
//                        min = t._2.d;
//                        minPathEdge = t;
//                    }
//                }
//            }
//            return new Tuple2<>(minPathEdge._2.d, new NodePathCost(destination, minPathEdge._1, minPathEdge._2.d));
//        }).sortByKey().values().filter((Function<NodePathCost, Boolean>) nodePathCost -> nodePathCost.n.equals(startNode) ? false : true).collect().forEach(System.out::println);



//                .collect().forEach(System.out::println);
//        step5.collect().forEach(System.out::println);
//        JavaPairRDD<String, Iterable<Edge>> step5 = step4.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Edge>>, String, Edge>() {
//            @Override
//            public Iterator<Tuple2<String, Edge>> call(Tuple2<String, Iterable<Edge>> input) throws Exception {
//                ArrayList<Tuple2<String, Edge>> ret = new ArrayList<>();
//                String[] parts = input._1.split("-");
//                String lastVisitedNode = parts[parts.length-1];
//                for (Edge e: input._2) {
//                    if (e == null) {
//                        continue;
//                    }
//                    Iterable<Edge> neighbours = graph.get(e.n);
//                    for (Edge neighbour : neighbours) {
//                        if (neighbour == null) {
//                            continue;
//                        }
//                        String path = String.format("-%s", neighbour.n);
//                        String newPath = input._1.concat(path);
//                        ret.add(new Tuple2<>(newPath, new Edge(neighbour.n, e.d + neighbour.d)));
//                    }
//                }
//                return ret.iterator();
//            }
//        }).groupByKey();
//        step5.collect().forEach(System.out::println);
//        step4.flatMapToPair(mapperFunction).groupByKey().collect().forEach(System.out::println);

//        step1.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Edge>>, String, Integer>() {
//            @Override
//            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Edge>> input) throws Exception {
//                ArrayList<Tuple2<String, Integer>> ret = new ArrayList<>();
//                for (Edge e : input._2) {
//                    ret.add(new Tuple2<>(e.n, e.d));
//                }
//                return ret.iterator();
//            }
//        });
//        step1.flatMap(new FlatMapFunction<Tuple2<String,Tuple2<String,Integer>>, ConnectedNode>() {
//            @Override
//            public Iterator<ConnectedNode> call(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
//                ArrayList<ConnectedNode> ret = new ArrayList<>();
//                String n1 = input._1;
//                String n2 = input._2._1;
//                Integer d = input._2._2;
//                ret.add(new ConnectedNode(n1, n2, d));
//                ret.add(new ConnectedNode(n2, null, null));
//                return ret.iterator();
//            }
//        });
//        JavaPairRDD<String, Edge> step2 = step1.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,Integer>>, String, Edge>() {
//            @Override
//            public Iterator<Tuple2<String, Edge>> call(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
//                ArrayList<Tuple2<String, Edge>> ret = new ArrayList<>();
//                String n1 = input._1;
//                String n2 = input._2._1;
//                Integer d = input._2._2;
//                ret.add(new Tuple2<>(n1, new Edge(n2, d)));
//                ret.add(new Tuple2<>(n2, null));
//                return ret.iterator();
//            }
//        });

//        // Get distances to reachable nodes
//        JavaPairRDD<String, Integer> step3 = step2.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Edge>>, String, Integer>() {
//
//            @Override
//            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Edge>> input) throws Exception {
//                ArrayList<Tuple2<String, Integer>> ret = new ArrayList<>();
//                String n1 = input._1;
//                for (Edge e : input._2) {
//                    ret.add(new Tuple2<>(e.n, e.d));
//                }
//                return ret.iterator();
//            }
//        });

        // Groups step 2 by key to get a list of adjacent nodes and reduces it so that it picks the minimum edge
        // for each node
//       JavaPairRDD<String, Edge> step3 = step2.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Edge>>, String, Edge>() {
//           @Override
//           public Tuple2<String, Edge> call(Tuple2<String, Iterable<Edge>> input) throws Exception {
//               String n1 = input._1;
//               Integer min = Integer.MAX_VALUE;
//               Edge minEdge = null;
//               Edge m = null;
//               for (Edge e: input._2) {
//                   if (e == null) {
//                       continue;
//                   }
//                   // get edge
//                   if (getEdge(n1, e.n, graph) != null) {
//                       m = getEdge(n1, e.n, graph);
//                   } else if (e.d < min) {
//                       min = e.d;
//                       minEdge = e;
//                   }
//               }
//               if (m != null) {
//                   m.d = min;
//               }
//               return new Tuple2<>(n1, m);
//           }
//       });


//        step2.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,Integer>>>, String, Integer>() {
//
//            @Override
//            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> input) throws Exception {
//                String n1 = input._1;
//                ArrayList<Tuple2<String, Integer>> ret = new ArrayList<>();
//                for (Tuple2<String, Integer> t : input._2) {
//                    if (t == null) {
//                        continue;
//                    }
//                    String n2 = t._1;
//                    Integer d = t._2;
//                    ret.add(new Tuple2<>(n2, d));
//                }
//
//            }
//        });

//        Map<String, Iterable<Edge>> graph  = step2.groupByKey().collectAsMap();
//        ArrayList<String> vertices = new ArrayList<>();
//        vertices.addAll(step2.groupByKey().sortByKey().keys().collect());
//
//        // child --> parent
//        Map<String, String> parent = new HashMap<>();
//        // Store minimum distance edges
//        Map<String, Integer> minDistances = new  HashMap<>();
//        // Track vertices picked to MST
//        Set<String> visited = new HashSet<>();
//
//        for (String v : vertices) {
//            minDistances.put(v,Integer.MAX_VALUE);
//        }
//
//        minDistances.put(vertices.get(0), 0);
//        PriorityQueue<String> q = new PriorityQueue<>();
//        q.add(vertices.get(0));
//        while (!q.isEmpty()) {
//            String v = q.poll();
//            visited.add(v);
//            if (!graph.containsKey(v)) {
//                continue;
//            }
//            Iterable<Edge> edges = graph.get(v);
//            for (Edge edge: edges) {
//                if (edge == null) {
//                    continue;
//                }
//                String neighbour = edge.n;
//                if (!visited.contains(neighbour)) {
//                    Integer d = edge.d;
//                    Integer newDistance = minDistances.get(v) + d;
//                    if (newDistance < minDistances.get(neighbour)) {
//                        minDistances.put(neighbour, newDistance);
//                        parent.put(neighbour, v);
//                    }
//                    q.add(neighbour);
//                }
//            }
//        }
//
//        for (String v : vertices) {
//            if (parent.get(v) == null) {
//                continue;
//            }
//            System.out.println(v + " " + minDistances.get(v));
//        }
    }
}
