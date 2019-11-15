import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class AssignmentOne {

    public static class Triple implements Serializable {
        String user;
        Integer r1;
        Integer r2;

        public Triple(String user, Integer r1, Integer r2) {
            this.user = user;
            this.r1 = r1;
            this.r2 = r2;
        }

        @Override
        public String toString() {
            return String.format("(%s, %d, %d)", this.user, this.r1, this.r2);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Assignment One")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> input = context.textFile("movie-ratings.txt");

        JavaPairRDD<String, Tuple2<String, Integer>> step1 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
                String[] parts = line.split("::");
                String user = parts[0];
                String movie = parts[1];
                Integer rating = Integer.parseInt(parts[2]);

                return new Tuple2<>(user, new Tuple2<>(movie, rating));
            }
        });
        step1.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Tuple2<String,Integer>>>, Tuple2<String, String>, Triple>() {
            @Override
            public Iterator<Tuple2<Tuple2<String, String>, Triple>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> input) throws Exception {
                ArrayList<Tuple2<Tuple2<String, String>, Triple>> ret = new ArrayList<>();
                String user = input._1;
                ArrayList<Tuple2<String, Integer>> movies = new ArrayList<>();
                // Add all movie ratings to movies array
                input._2.forEach((movies::add));
                for (int i = 0; i < movies.size() - 1; i++) {
                    for (int j = i+1; j < movies.size(); j++) {
                        String m1 = movies.get(i)._1;
                        Integer r1 = movies.get(i)._2;
                        String m2 = movies.get(j)._1;
                        Integer r2 = movies.get(j)._2;
                        Tuple2<String, String> moviePair;
                        if (m1.compareTo(m2) > 0) {
                            moviePair = new Tuple2<>(m1, m2);
                        } else {
                            moviePair = new Tuple2<>(m2, m1);
                            Integer swap = r1;
                            r1 = r2;
                            r2 = swap;
                        }
                        Triple triple = new Triple(user, r1, r2);
                        ret.add(new Tuple2<>(moviePair, triple));
                    }
                }
                return ret.iterator();
            }
        }).groupByKey().saveAsTextFile("output");
//                .collect().forEach(System.out::println);
//        step1.collect().forEach(System.out::println);
    }
}
