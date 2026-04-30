package edu.ds200.lab03.task3;

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

/**
 * ĐTB riêng Nam / Nữ cho từng phim (một dòng mỗi phim; thiếu chiều giới tính → N/A).
 */
public class Task3GenderRatings {

  private static final class Side {
    double sum;
    long count;
  }

  private static final class Both {
    Side male;
    Side female;
  }

  public static void main(String[] args) throws Exception {
    String labRoot = args.length > 0 ? args[0] : ".";
    Path outFile = Paths.get(args.length > 1 ? args[1] : "output/task3.txt");

    SparkConf conf = new SparkConf().setAppName("Lab03-Task3-GenderRatings");
    try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
      Path dataDir = Paths.get(labRoot, "Data");

      JavaRDD<String> movieLines = jsc.textFile(dataDir.resolve("movies.txt").toUri().toString());
      Map<Integer, String> movieIdToTitle = new HashMap<>();
      for (String line : movieLines.collect()) {
        String[] p = line.split(",", 3);
        movieIdToTitle.put(Integer.parseInt(p[0].trim()), p.length > 1 ? p[1].trim() : "?");
      }

      JavaRDD<String> userLines = jsc.textFile(dataDir.resolve("users.txt").toUri().toString());
      JavaPairRDD<Integer, String> userGender =
          userLines.mapToPair(
              line -> {
                String[] p = line.split(",");
                return new Tuple2<>(Integer.parseInt(p[0].trim()), p[1].trim());
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

      JavaPairRDD<String, Tuple2<Double, Long>> byMovieGender =
          ratingsByUser
              .join(userGender)
              .filter(t -> "M".equals(t._2._2) || "F".equals(t._2._2))
              .mapToPair(
                  t -> {
                    int movieId = t._2._1._1;
                    double rating = t._2._1._2;
                    String gender = t._2._2;
                    return new Tuple2<>(movieId + "|" + gender, new Tuple2<>(rating, 1L));
                  })
              .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      Map<Integer, Both> rollup = new HashMap<>();
      for (Tuple2<String, Tuple2<Double, Long>> row : byMovieGender.collect()) {
        String[] key = row._1.split("\\|", 2);
        int movieId = Integer.parseInt(key[0]);
        String g = key[1];
        Both b = rollup.computeIfAbsent(movieId, k -> new Both());
        Side s = new Side();
        s.sum = row._2._1;
        s.count = row._2._2;
        if ("M".equals(g)) {
          b.male = s;
        } else {
          b.female = s;
        }
      }

      List<Integer> movieIds = new ArrayList<>(rollup.keySet());
      movieIds.sort(
          Comparator.comparing(
                  (Integer id) -> movieIdToTitle.getOrDefault(id, "").toLowerCase())
              .thenComparingInt(id -> id));

      List<String> out = new ArrayList<>();
      out.add("=== Bài 3: ĐTB theo phim — Nam / Nữ ===");
      out.add("MovieID\tTitle\tMaleAvg\tMaleCount\tFemaleAvg\tFemaleCount");
      for (Integer mid : movieIds) {
        Both b = rollup.get(mid);
        String title = movieIdToTitle.getOrDefault(mid, "?");
        String mAvg = "N/A";
        long mCnt = 0;
        if (b.male != null && b.male.count > 0) {
          mAvg = RatingFormat.fourDecimals(b.male.sum / b.male.count);
          mCnt = b.male.count;
        }
        String fAvg = "N/A";
        long fCnt = 0;
        if (b.female != null && b.female.count > 0) {
          fAvg = RatingFormat.fourDecimals(b.female.sum / b.female.count);
          fCnt = b.female.count;
        }
        out.add(String.format("%d\t%s\t%s\t%d\t%s\t%d", mid, title, mAvg, mCnt, fAvg, fCnt));
      }

      IoUtil.writeLines(outFile, out);
    }
  }
}
