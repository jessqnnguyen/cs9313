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
import java.util.*;

/**
 * A movie rating aggregator which aggregates user movie rating scores into pairs.
 */
public class MovieRatingAggregator {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "get movie pairs to user ratings");
        job1.setMapperClass(UserIdToMovieRatingMapper.class);
        job1.setReducerClass(MoviePairsToUserRatingPairsReducer.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setMapOutputValueClass(MovieRatingPair.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(UserRatingPair.class);
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
        job2.setMapOutputValueClass(UserRatingPair.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1], "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1], "out2"));
        job2.waitForCompletion(true);
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

    }

    /**
     * Mapper for the first MapReduce job  which reads from the raw input text file and outputs
     * movie rating pairs keyed by user IDs.
     */
    static class UserIdToMovieRatingMapper extends Mapper<LongWritable, Text, Text, MovieRatingPair> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] params = value.toString().split("::");

            MovieRatingPair output = new MovieRatingPair(new Text(params[1]), new Text(params[2]));
            context.write(new Text(params[0]), output);
        }
    }

    /**
     * Reducer for first MapReduce job which reorders the output of the first job's mapping of
     * movie rating pairs keyed by user ids to all possible movie rating pairs for that user.
     * E.g.
     * input:
     *   u1 -> (m1, 2), (m2, 3), (m3, 4)
     * output: (will output 3 rows for this input)
     *    (m1, m2) -> (u1, 2, 3),
     *    (m2, m3) -> (u1, 3, 4),
     *    (m1, m3) -> (u1, 2, 4)
     *
     */
    static class MoviePairsToUserRatingPairsReducer extends Reducer<Text, MovieRatingPair, Text, UserRatingPair> {

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

                        context.write(new Text(pair), new UserRatingPair(userId, mrp1.rating, mrp2.rating));
                    }
                }
            }
        }
    }

    /**
     * Mapper for the second MapReduce job.
     * Maps the output from the first MapReduce job to user rating pairs keyed by movie ID pairs.
     */
    static class MoviePairsMapper extends Mapper<LongWritable, Text, Text, UserRatingPair> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] params = value.toString().split("::");
            String[] userRatingPairParams = params[1]
                    .trim()
                    .replace("(", "")
                    .replace(")", "")
                    .split(",");
            Text userId = new Text(userRatingPairParams[0].trim());
            Text mr1 = new Text(userRatingPairParams[1].trim());
            Text mr2 = new Text(userRatingPairParams[2].trim());
            context.write(new Text(params[0]), new UserRatingPair(userId, mr1, mr2));
        }
    }

    /**
     * Reducer for the second MapReduce job.
     * Reduces the output from the second MapReduce job's mapper by
     * aggregating all user rating pairs keyed by movie ID pairs.
     */
    static class GroupMoviePairs extends Reducer<Text, UserRatingPair, Text, UserRatingPairArray> {

        @Override
        protected void reduce(Text moviePair, Iterable<UserRatingPair> userRatings, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            List<UserRatingPair> cache = new ArrayList<>();
            Iterator<UserRatingPair> it = userRatings.iterator();
            while (it.hasNext()) {
                UserRatingPair userRating = ReflectionUtils.newInstance(UserRatingPair.class, conf);
                ReflectionUtils.copy(conf, it.next(), userRating);
                cache.add(userRating);
            }
            Collections.sort(cache);
            UserRatingPair[] urps = new UserRatingPair[cache.size()];
            int i = 0;
            for (UserRatingPair urp : cache) {
                urps[i] = urp;
                i++;
            }

            context.write(moviePair, new UserRatingPairArray(urps));

        }
    }

    /**
     * Data Classes
     */

    /**
     * An ArrayWritable which stores the final output value array of user rating pairs.
     */
    static class UserRatingPairArray extends ArrayWritable {

        public UserRatingPairArray() {
            super(UserRatingPair.class);
        }

        public UserRatingPairArray(UserRatingPair[] values) {
            super(UserRatingPairArray.class, values);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();

            b.append("[");
            for (String s: super.toStrings()) {
                if (b.length() == 1) {
                    b.append(s);
                } else {
                    b.append("," + s);
                }
            }
            b.append("]");
            return b.toString();
        }

    }

    /** User rating pair output */
    static class UserRatingPair implements Writable, Comparable {
        Text userId;
        /** User Rating for 1st Movie Rating in Pair */
        Text mr1;
        /** User Rating for 2nd Movie Rating in Pair */
        Text mr2;

        public UserRatingPair() {
            this.userId = new Text();
            this.mr1 = new Text();
            this.mr2 = new Text();
        }

        public UserRatingPair(String userId, String mr1, String mr2) {
            this.userId = new Text(userId);
            this.mr1 = new Text(mr1);
            this.mr2 = new Text(mr2);
        }

        public UserRatingPair(Text userId, Text mr1, Text mr2) {
            this.userId = userId;
            this.mr1 = mr1;
            this.mr2 = mr2;
        }

        @Override
        public String toString() {
            return String.format("(%s, %s, %s)", userId, mr1, mr2);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            userId.write(dataOutput);
            mr1.write(dataOutput);
            mr2.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            userId.readFields(dataInput);
            mr1.readFields(dataInput);
            mr2.readFields(dataInput);
        }

        @Override
        public int compareTo(Object o) {
            UserRatingPair other = (UserRatingPair) o;
            String thisValue = this.userId.toString();
            String thatValue = other.userId.toString();
            return (thisValue.compareTo(thatValue) > 0) ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }

    /** Movie rating pair output of the first job's mapper. */
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
        }
    }

}
