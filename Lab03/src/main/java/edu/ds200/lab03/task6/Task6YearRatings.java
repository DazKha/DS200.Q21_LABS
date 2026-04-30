package edu.ds200.lab03.task6;

import edu.ds200.lab03.util.IoUtil;
import edu.ds200.lab03.util.RatingFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Task6YearRatings {
  static int yearFromUnixSeconds(long ts) {
    return Instant.ofEpochSecond(ts).atZone(ZoneOffset.UTC).getYear();
  }

  public static void main(String[] args) throws Exception {
    String labRoot = args.length > 0 ? args[0] : ".";
    Path outFile = Paths.get(args.length > 1 ? args[1] : "output/task6.txt");

    SparkConf conf = new SparkConf().setAppName("Lab03-Task6-YearRatings");
    try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
      Path dataDir = Paths.get(labRoot, "Data");

      JavaRDD<String> r1 = jsc.textFile(dataDir.resolve("ratings_1.txt").toUri().toString());
      JavaRDD<String> r2 = jsc.textFile(dataDir.resolve("ratings_2.txt").toUri().toString());
      JavaRDD<String> ratings = r1.union(r2);

      JavaPairRDD<Integer, Tuple2<Double, Long>> yearSumCount =
          ratings
              .mapToPair(
                  line -> {
                    String[] p = line.split(",");
                    double rating = Double.parseDouble(p[2].trim());
                    long ts = Long.parseLong(p[3].trim());
                    int year = yearFromUnixSeconds(ts);
                    return new Tuple2<>(year, new Tuple2<>(rating, 1L));
                  })
              .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<Tuple2<Integer, Tuple2<Double, Long>>> rows = yearSumCount.collect();

      List<String> out = new ArrayList<>();
      out.add("=== Bài 6: Tổng lượt đánh giá & đTB theo năm ===");
      out.add("Year\tAvgRating\tTotalRatings");
      rows.stream()
          .sorted(Comparator.comparingInt(Tuple2::_1))
          .forEach(
              row -> {
                double avg = row._2._1 / row._2._2;
                out.add(
                    String.format(
                        "%d\t%s\t%d", row._1, RatingFormat.fourDecimals(avg), row._2._2));
              });

      IoUtil.writeLines(outFile, out);
    }
  }
}
