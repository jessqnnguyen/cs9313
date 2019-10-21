import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MovieRatingAggregator {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "get movie pair to user ratings");
        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setMapOutputValueClass(MovieRatingPair.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1], "out1"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "group movie pairs to user ratings");
        job2.setMapperClass(MoviePairsMapper.class);
        job2.setReducerClass(GroupMoviePairs.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
//        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
//        job2.setSortComparatorClass(MyComparator.class);
        FileInputFormat.addInputPath(job2, new Path(args[1], "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1], "out2"));
        job2.waitForCompletion(true);
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

    }

    static class MoviePairsMapper extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] params = value.toString().split("::");

            context.write(new Text(params[0]), new Text(params[1].trim()));
        }
    }

    static class GroupMoviePairs extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text moviePair, Iterable<Text> userRatings, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            Configuration conf = context.getConfiguration();
            List<Text> cache = new ArrayList<>();
            Iterator<Text> it = userRatings.iterator();
            while (it.hasNext()) {
                Text userRating = ReflectionUtils.newInstance(Text.class, conf);
                ReflectionUtils.copy(conf, it.next(), userRating);
                cache.add(userRating);
            }
            StringBuilder b = new StringBuilder();
            b.append("[");
            for (Text t: cache) {
                if (b.length() == 1) {
                    b.append(t.toString());
                } else {
                    b.append("," + t.toString());
                }
            }
            b.append("]");

            context.write(moviePair, new Text(b.toString()));

        }
    }

    static class UserRatingArrayWritable extends ArrayWritable {

        public UserRatingArrayWritable(Class<? extends Writable> valueClass) {
            super(valueClass);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();

            for (String userIdRating: super.toStrings()) {
                if (b.length() == 0) {
                    b.append("[" + userIdRating + "]");
                } else {
                    b.append("," + "[" + userIdRating + "]");
                }
            }
            return b.toString();
        }
    }
    
    static class MyMapper extends Mapper<LongWritable, Text, Text, MovieRatingPair> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
            String[] params = value.toString().split("::");

            MovieRatingPair output = new MovieRatingPair(new Text(params[1]), new Text(params[2]));
            context.write(new Text(params[0]), output);
        }
    }

    static class MovieRatingPair implements Writable {
        Text movieId;
        Text rating;

        public MovieRatingPair() {
            this.movieId = new Text();
            this.rating = new Text();
        }

        public MovieRatingPair(Text movieId, Text rating) {
            this.movieId = movieId;
            this.rating = rating;
        }

        @Override
        public String toString() {
            return movieId + "," + rating;

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            movieId.write(dataOutput);
            rating.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            movieId.readFields(dataInput);
            rating.readFields(dataInput);
        }
    }

    static class MyReducer extends Reducer<Text, MovieRatingPair, Text, Text> {

        @Override
        protected void reduce(Text userId, Iterable<MovieRatingPair> userIdRatings, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            List<MovieRatingPair> cache = new ArrayList<>();
            Iterator<MovieRatingPair> it = userIdRatings.iterator();
            while (it.hasNext()) {
                MovieRatingPair mrp = ReflectionUtils.newInstance(MovieRatingPair.class, conf);
                ReflectionUtils.copy(conf, it.next(), mrp);
                cache.add(mrp);
            }

            MovieRatingPair[] arr = new MovieRatingPair[cache.size()];
            if(cache.size() >= 2) {
                for(int i = 0; i < cache.size(); i++) {
                    for (int j = i+1; j < cache.size(); j++ ) {
                        String m1 = cache.get(i).movieId.toString();
                        String m2 = cache.get(j).movieId.toString();
                        String pair;
                        int result = m1.compareTo(m2);
                        if (result < 0) {
                            pair = m1 + "," + m2 + "::";
                        } else {
                            pair = m2 + "," + m1 + "::";
                        }

                        MovieRatingPair mrp1 = cache.get(i);
                        MovieRatingPair mrp2 = cache.get(j);

                        context.write(new Text(pair), new Text(String.format("(%s, %s, %s)", userId, mrp1.rating, mrp2.rating)));
                    }
                }
            }
//            for (MovieRatingPair urp : cache) {
//                if (urp != null) {
//                    arr[i] = urp;
//                }
//                i++;
//            }
//            UserRatingArrayWritable output = new UserRatingArrayWritable(MovieRatingPair.class);
//            output.set(arr);

//        	context.write(movieId, output);
        }
    }

    static class MyComparator extends WritableComparator {

        public MyComparator() {
            super(Text.class, true);
        }

        protected MyComparator(Class<? extends WritableComparable> keyClass) {
            super(keyClass);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text aa = (Text) a;
            Text bb = (Text) b;
            Integer a1 = Integer.parseInt(aa.toString());
            Integer a2 = Integer.parseInt(aa.toString());
            return a1.compareTo(a2);
//            return -1 * aa.compareTo(bb);
        }
    }

}
