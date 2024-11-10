import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MusicReleaseAnalysis extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MusicReleaseAnalysis(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Music Release Analysis");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MusicReleaseMapper.class);
        job.setReducerClass(MusicReleaseReducer.class);
        job.setCombinerClass(MusicReleaseCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MusicReleaseMapper extends Mapper<Object, Text, Text, SumCount> {
        private final Text labelArtistDecadeKey = new Text();
        private final SumCount sumCount = new SumCount();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\u0001");
            if (fields.length >= 12) {
                String labelId = fields[6];
                String artistId = fields[4];
                String artistName = fields[5];
                String genre = fields[8];
                String releaseDate = fields[11];

                int decade = Integer.parseInt(releaseDate.substring(0, 3)) * 10;

                labelArtistDecadeKey.set(labelId + "\t" + artistId + "\t" + artistName + "\t" + decade);
                sumCount.set(new IntWritable(1), genre);
                context.write(labelArtistDecadeKey, sumCount);
            }
        }
    }

    public static class MusicReleaseReducer extends Reducer<Text, SumCount, Text, Text> {
        private final Text resultValue = new Text();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            int albumCount = 0;
            Set<String> uniqueGenres = new HashSet<>();

            for (SumCount val : values) {
                albumCount += val.getCount().get();
                uniqueGenres.add(val.getGenre());
            }

            // Use tab as the delimiter in the output
            String result = albumCount + "\t" + String.join(",", uniqueGenres);
            resultValue.set(result);
            context.write(key, resultValue);
        }
    }


    public static class MusicReleaseCombiner extends Reducer<Text, SumCount, Text, SumCount> {
        private final SumCount sumCount = new SumCount();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            int albumCount = 0;
            Set<String> genres = new HashSet<>();

            for (SumCount val : values) {
                albumCount += val.getCount().get();
                genres.add(val.getGenre());
            }

            sumCount.set(new IntWritable(albumCount), String.join(",", genres));
            context.write(key, sumCount);
        }
    }
}
