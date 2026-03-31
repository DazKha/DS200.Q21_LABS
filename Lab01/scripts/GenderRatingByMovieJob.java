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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderRatingByMovieJob {
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

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {
        private final Map<String, String> userGender = new HashMap<>();
        private final Map<String, String> movieTitles = new HashMap<>();
        private final Text movieOut = new Text();
        private final Text genderRatingOut = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                String name = path.getName();
                if (name.contains("users")) {
                    loadUsers(path.toString());
                } else if (name.contains("movies")) {
                    loadMovies(path.toString());
                }
            }
        }

        private void loadUsers(String filePath) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s*,\\s*");
                    if (parts.length < 2) {
                        continue;
                    }
                    userGender.put(parts[0], parts[1]);
                }
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
                String userId = parts[0];
                String movieId = parts[1];
                float rating = Float.parseFloat(parts[2]);

                String gender = userGender.get(userId);
                String movieTitle = movieTitles.get(movieId);
                if (gender == null || movieTitle == null) {
                    return;
                }

                movieOut.set(movieTitle);
                genderRatingOut.set(gender + ":" + rating);
                context.write(movieOut, genderRatingOut);
            } catch (NumberFormatException ignored) {
                // Skip malformed rows.
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float maleSum = 0.0f;
            int maleCount = 0;
            float femaleSum = 0.0f;
            int femaleCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length != 2) {
                    continue;
                }

                try {
                    float rating = Float.parseFloat(parts[1]);
                    if ("M".equalsIgnoreCase(parts[0])) {
                        maleSum += rating;
                        maleCount++;
                    } else if ("F".equalsIgnoreCase(parts[0])) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                } catch (NumberFormatException ignored) {
                    // Skip malformed rows.
                }
            }

            float maleAvg = maleCount == 0 ? 0.0f : maleSum / maleCount;
            float femaleAvg = femaleCount == 0 ? 0.0f : femaleSum / femaleCount;
            String line = String.format(
                    Locale.US,
                    "%s: Male_Avg=%.2f, Female_Avg=%.2f",
                    key.toString(), maleAvg, femaleAvg
            );
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: GenderRatingByMovieJob <ratings_1> <ratings_2> <users> <movies> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating By Movie");
        job.setJarByClass(GenderRatingByMovieJob.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPaths(job, args[0] + "," + args[1]);
        Path outputPath = new Path(args[4]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        boolean success = job.waitForCompletion(true);
        if (success) {
            writeResultTxt(conf, outputPath, "bai3_result.txt");
        }
        System.exit(success ? 0 : 1);
    }
}
