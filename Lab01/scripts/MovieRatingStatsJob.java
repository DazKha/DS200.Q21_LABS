import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingStatsJob {
    private static void writeResultTxt(Configuration conf, Path outputDir, String fileName) throws IOException {
        FileSystem fs = outputDir.getFileSystem(conf);
        Path partFile = new Path(outputDir, "part-r-00000");
        if (!fs.exists(partFile)) {
            return;
        }

        Path txtFile = new Path(outputDir, fileName);
        List<String> lines = new ArrayList<>();
        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(partFile), StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        Collections.sort(lines);
        try (
                BufferedWriter partWriter = new BufferedWriter(
                        new OutputStreamWriter(fs.create(partFile, true), StandardCharsets.UTF_8));
                BufferedWriter txtWriter = new BufferedWriter(
                        new OutputStreamWriter(fs.create(txtFile, true), StandardCharsets.UTF_8))
        ) {
            for (String line : lines) {
                partWriter.write(line);
                partWriter.newLine();
                txtWriter.write(line);
                txtWriter.newLine();
            }
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private final Text movieId = new Text();
        private final FloatWritable ratingOut = new FloatWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\\s*,\\s*");
            if (parts.length < 3) {
                return;
            }

            try {
                movieId.set(parts[1]);
                ratingOut.set(Float.parseFloat(parts[2]));
                context.write(movieId, ratingOut);
            } catch (NumberFormatException ignored) {
                // Skip malformed rows.
            }
        }
    }

    public static class MovieStatsReducer extends Reducer<Text, FloatWritable, Text, NullWritable> {
        private final Map<String, String> movieTitles = new HashMap<>();
        private String maxMovie = "";
        private float maxRating = -1.0f;
        private int maxCount = 0;

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                if (!path.getName().contains("movies")) {
                    continue;
                }
                loadMovies(path.toString());
            }
        }

        private void loadMovies(String filePath) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parsed = parseMovieLine(line);
                    if (parsed == null) {
                        continue;
                    }
                    movieTitles.put(parsed[0], parsed[1]);
                }
            }
        }

        private String[] parseMovieLine(String line) {
            int firstComma = line.indexOf(',');
            int lastComma = line.lastIndexOf(',');
            if (firstComma <= 0 || lastComma <= firstComma) {
                return null;
            }

            String id = line.substring(0, firstComma).trim();
            String title = line.substring(firstComma + 1, lastComma).trim();
            return new String[]{id, title};
        }

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            int count = 0;
            for (FloatWritable rating : values) {
                sum += rating.get();
                count++;
            }

            if (count == 0) {
                return;
            }

            float avg = sum / count;
            String title = movieTitles.getOrDefault(key.toString(), key.toString());
            String line = String.format(
                    Locale.US,
                    "%s AverageRating: %.2f (TotalRatings: %d)",
                    title, avg, count
            );
            context.write(new Text(line), NullWritable.get());

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
                maxCount = count;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String summary = String.format(
                        Locale.US,
                        "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings. (TotalRatings: %d)",
                        maxMovie, maxRating, maxCount
                );
                context.write(new Text(summary), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: MovieRatingStatsJob <ratings_1> <ratings_2> <movies> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Stats");
        job.setJarByClass(MovieRatingStatsJob.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(MovieStatsReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPaths(job, args[0] + "," + args[1]);
        Path outputPath = new Path(args[3]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.addCacheFile(new Path(args[2]).toUri());

        boolean success = job.waitForCompletion(true);
        if (success) {
            writeResultTxt(conf, outputPath, "bai1_result.txt");
        }
        System.exit(success ? 0 : 1);
    }
}
