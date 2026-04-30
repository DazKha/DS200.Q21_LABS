package edu.ds200.lab03.task4;

import edu.ds200.lab03.util.AgeGrouping;
import edu.ds200.lab03.util.IoUtil;
import edu.ds200.lab03.util.RatingFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** ĐTB theo phim và nhóm tuổi 0–18, 19–35, 36–50, 51+. */
public class Task4AgeGroupRatings {

  public static void main(String[] args) throws Exception {
    String labRoot = args.length > 0 ? args[0] : ".";
    Path outFile = Paths.get(args.length > 1 ? args[1] : "output/task4.txt");

    SparkConf conf = new SparkConf().setAppName("Lab03-Task4-AgeGroupRatings");
    try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
      Path dataDir = Paths.get(labRoot, "Data");

      Map<Integer, String> movieTitles = new HashMap<>();
      for (String line : jsc.textFile(dataDir.resolve("movies.txt").toUri().toString()).collect()) {
        String[] p = line.split(",", 3);
        movieTitles.put(Integer.parseInt(p[0].trim()), p.length > 1 ? p[1].trim() : "?");
      }

      JavaRDD<String> userLines = jsc.textFile(dataDir.resolve("users.txt").toUri().toString());
      JavaPairRDD<Integer, String> userAgeGroup =
          userLines.mapToPair(
              line -> {
                String[] p = line.split(",", 5);
                int uid = Integer.parseInt(p[0].trim());
                int age = Integer.parseInt(p[2].trim());
                return new Tuple2<>(uid, AgeGrouping.bucket(age));
              });

      JavaRDD<String> r1 = jsc.textFile(dataDir.resolve("ratings_1.txt").toUri().toString());
      JavaRDD<String> r2 = jsc.textFile(dataDir.resolve("ratings_2.txt").toUri().toString());
      JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsByUser =
          r1.union(r2)
              .mapToPair(
                  line -> {
                    String[] p = line.split(",");
                    int userId = Integer.parseInt(p[0].trim());
                    int movieId = Integer.parseInt(p[1].trim());
                    double rating = Double.parseDouble(p[2].trim());
                    return new Tuple2<>(userId, new Tuple2<>(movieId, rating));
                  });

      JavaPairRDD<String, Tuple2<Double, Long>> keyMovieAge =
          ratingsByUser
              .join(userAgeGroup)
              .mapToPair(
                  t -> {
                    int movieId = t._2._1._1;
                    double rating = t._2._1._2;
                    String grp = t._2._2;
                    return new Tuple2<>(movieId + "\t" + grp, new Tuple2<>(rating, 1L));
                  })
              .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      Map<String, Integer> cohortOrder = new HashMap<>(4);
      cohortOrder.put("0-18", 0);
      cohortOrder.put("19-35", 1);
      cohortOrder.put("36-50", 2);
      cohortOrder.put("51+", 3);

      List<Tuple2<String, Tuple2<Double, Long>>> sortedRows =
          new ArrayList<>(keyMovieAge.collect());
      sortedRows.sort(
          Comparator.comparing(
                  (Tuple2<String, Tuple2<Double, Long>> row) -> {
                    int mid = Integer.parseInt(row._1.split("\t", 2)[0]);
                    return movieTitles.getOrDefault(mid, "?").toLowerCase();
                  })
              .thenComparingInt(
                  row -> cohortOrder.getOrDefault(row._1.split("\t", 2)[1], 99)));

      List<String> out = new ArrayList<>();
      out.add("=== Bài 4: ĐTB theo phim và nhóm tuổi (0-18 · 19-35 · 36-50 · 51+) ===");
      out.add("MovieID\tTitle\tAgeGroup\tAvgRating\tTotalRatings");
      for (Tuple2<String, Tuple2<Double, Long>> row : sortedRows) {
        String[] parts = row._1.split("\t", 2);
        int movieId = Integer.parseInt(parts[0]);
        String grp = parts[1];
        long n = row._2._2;
        double avg = row._2._1 / n;
        out.add(
            String.format(
                "%d\t%s\t%s\t%s\t%d",
                movieId,
                movieTitles.getOrDefault(movieId, "?"),
                grp,
                RatingFormat.fourDecimals(avg),
                n));
      }

      IoUtil.writeLines(outFile, out);
    }
  }
}
