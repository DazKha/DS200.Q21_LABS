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

public class GenreRatingAnalysisJob {
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

    public static class GenreMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private final Map<String, String[]> movieGenres = new HashMap<>();
        private final Text genreOut = new Text();
        private final FloatWritable ratingOut = new FloatWritable();

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
                    movieGenres.put(parsed[0], parsed[2].split("\\|"));
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
            String genres = line.substring(lastComma + 1).trim();
            return new String[]{id, title, genres};
        }

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
                String movieId = parts[1];
                float rating = Float.parseFloat(parts[2]);
                String[] genres = movieGenres.get(movieId);
                if (genres == null) {
                    return;
                }

                ratingOut.set(rating);
                for (String genre : genres) {
                    genreOut.set(genre.trim());
                    context.write(genreOut, ratingOut);
                }
            } catch (NumberFormatException ignored) {
                // Skip malformed rows.
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, FloatWritable, Text, NullWritable> {
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
            String line = String.format(
                    Locale.US,
                    "%s: %.2f (TotalRatings: %d)",
                    key.toString(), avg, count
            );
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: GenreRatingAnalysisJob <ratings_1> <ratings_2> <movies> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        job.setJarByClass(GenreRatingAnalysisJob.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);
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
            writeResultTxt(conf, outputPath, "bai2_result.txt");
        }
        System.exit(success ? 0 : 1);
    }
}
