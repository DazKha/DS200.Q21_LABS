package edu.ds200.lab03.task1;

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
import java.util.List;
import java.util.Map;

/**
 * ĐTB & số lượt theo phim; phim ĐTB cao nhất trong các phim đủ ngưỡng lượt đánh giá.
 * Tham số tùy chọn: {@code args[2]} = ngưỡng tối thiểu cho bước “top” (mặc định 5). Đặt 50 nếu
 * cần khớp đúng câu “ít nhất 50 lượt” trong một phiên bản đề.
 */
public class Task1MovieRatings {

  public static void main(String[] args) throws Exception {
    String labRoot = args.length > 0 ? args[0] : ".";
    Path outFile = Paths.get(args.length > 1 ? args[1] : "output/task1.txt");
    int minRatingsForTop = 5;
    if (args.length >= 3) {
      minRatingsForTop = Integer.parseInt(args[2].trim());
    }

    SparkConf conf = new SparkConf().setAppName("Lab03-Task1-MovieRatings");
    try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
      Path dataDir = Paths.get(labRoot, "Data");

      JavaRDD<String> movieLines = jsc.textFile(dataDir.resolve("movies.txt").toUri().toString());
      Map<Integer, String> movieIdToTitle =
          movieLines
              .mapToPair(
                  line -> {
                    String[] p = line.split(",", 3);
                    int id = Integer.parseInt(p[0].trim());
                    String title = p.length > 1 ? p[1].trim() : "";
                    return new Tuple2<>(id, title);
                  })
              .collectAsMap();

      JavaRDD<String> r1 = jsc.textFile(dataDir.resolve("ratings_1.txt").toUri().toString());
      JavaRDD<String> r2 = jsc.textFile(dataDir.resolve("ratings_2.txt").toUri().toString());

      JavaPairRDD<Integer, Tuple2<Double, Long>> sumCount =
          r1.union(r2)
              .mapToPair(
                  line -> {
                    String[] p = line.split(",");
                    int movieId = Integer.parseInt(p[1].trim());
                    double rating = Double.parseDouble(p[2].trim());
                    return new Tuple2<>(movieId, new Tuple2<>(rating, 1L));
                  })
              .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<Tuple2<Integer, Tuple2<Double, Long>>> collected = new ArrayList<>(sumCount.collect());
      collected.sort(
          Comparator.comparing(
                  (Tuple2<Integer, Tuple2<Double, Long>> t) ->
                      movieIdToTitle.getOrDefault(t._1, "").toLowerCase())
              .thenComparingInt(Tuple2::_1));

      List<String> out = new ArrayList<>();
      out.add("=== Bài 1: ĐTB & tổng lượt đánh giá theo phim ===");
      out.add("MovieID\tTitle\tAvgRating\tTotalRatings");

      Comparator<Tuple2<Integer, Tuple2<Double, Long>>> chooseTop =
          Comparator.comparingDouble(
                  (Tuple2<Integer, Tuple2<Double, Long>> t) ->
                      t._2._1 / (double) t._2._2)
              .thenComparingLong(t -> t._2._2);

      Tuple2<Integer, Tuple2<Double, Long>> bestAbove = null;

      for (Tuple2<Integer, Tuple2<Double, Long>> row : collected) {
        int mid = row._1;
        long cnt = row._2._2;
        double avg = row._2._1 / cnt;
        String title = movieIdToTitle.getOrDefault(mid, "?");
        out.add(
            String.format(
                "%d\t%s\t%s\t%d",
                mid, title, RatingFormat.fourDecimals(avg), cnt));

        if (cnt >= minRatingsForTop) {
          if (bestAbove == null || chooseTop.compare(row, bestAbove) > 0) {
            bestAbove = row;
          }
        }
      }

      out.add("");
      out.add(
          String.format(
              "=== Phim ĐTB cao nhất trong các phim có ≥ %d lượt đánh giá ===",
              minRatingsForTop));
      if (bestAbove == null) {
        out.add("Không có phim đạt ngưỡng.");
      } else {
        long c = bestAbove._2._2;
        double avg = bestAbove._2._1 / c;
        out.add(
            String.format(
                "MovieID=%d | Title=%s | AvgRating=%s | TotalRatings=%d",
                bestAbove._1,
                movieIdToTitle.getOrDefault(bestAbove._1, "?"),
                RatingFormat.fourDecimals(avg),
                c));
      }

      IoUtil.writeLines(outFile, out);
    }
  }
}
