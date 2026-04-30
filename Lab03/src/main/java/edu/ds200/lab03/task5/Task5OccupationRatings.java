package edu.ds200.lab03.task5;

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

/** Trung bình rating và số lượt theo tên nghề (Occupation name). */
public class Task5OccupationRatings {

  public static void main(String[] args) throws Exception {
    String labRoot = args.length > 0 ? args[0] : ".";
    Path outFile = Paths.get(args.length > 1 ? args[1] : "output/task5.txt");

    SparkConf conf = new SparkConf().setAppName("Lab03-Task5-OccupationRatings");
    try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
      Path dataDir = Paths.get(labRoot, "Data");

      Map<Integer, String> occIdToName = new HashMap<>();
      for (String line :
          jsc.textFile(dataDir.resolve("occupation.txt").toUri().toString()).collect()) {
        String[] p = line.split(",", 2);
        if (p.length >= 2) {
          occIdToName.put(Integer.parseInt(p[0].trim()), p[1].trim());
        }
      }

      JavaRDD<String> userLines = jsc.textFile(dataDir.resolve("users.txt").toUri().toString());
      JavaPairRDD<Integer, Integer> userOcc =
          userLines.mapToPair(
              line -> {
                String[] p = line.split(",");
                int uid = Integer.parseInt(p[0].trim());
                int occ = Integer.parseInt(p[3].trim());
                return new Tuple2<>(uid, occ);
              });

      JavaRDD<String> r1 = jsc.textFile(dataDir.resolve("ratings_1.txt").toUri().toString());
      JavaRDD<String> r2 = jsc.textFile(dataDir.resolve("ratings_2.txt").toUri().toString());
      JavaPairRDD<Integer, Double> ratingByUser =
          r1.union(r2)
              .mapToPair(
                  line -> {
                    String[] p = line.split(",");
                    int userId = Integer.parseInt(p[0].trim());
                    double rating = Double.parseDouble(p[2].trim());
                    return new Tuple2<>(userId, rating);
                  });

      JavaPairRDD<Integer, Tuple2<Double, Long>> occSumCount =
          ratingByUser
              .join(userOcc)
              .mapToPair(t -> new Tuple2<>(t._2._2, new Tuple2<>(t._2._1, 1L)))
              .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<Tuple2<Integer, Tuple2<Double, Long>>> rows = occSumCount.collect();
      rows.sort(
          Comparator.comparing(
                  (Tuple2<Integer, Tuple2<Double, Long>> row) ->
                      occIdToName.getOrDefault(row._1, "?").toLowerCase())
              .thenComparingInt(Tuple2::_1));

      List<String> out = new ArrayList<>();
      out.add("=== Bài 5: ĐTB & tổng lượt đánh giá theo nghề (Occupation) ===");
      out.add("Occupation\tAverageRating\tTotalRatings");
      for (Tuple2<Integer, Tuple2<Double, Long>> row : rows) {
        double avg = row._2._1 / row._2._2;
        out.add(
            String.format(
                "%s\t%s\t%d",
                occIdToName.getOrDefault(row._1, "?"),
                RatingFormat.fourDecimals(avg),
                row._2._2));
      }

      IoUtil.writeLines(outFile, out);
    }
  }
}
