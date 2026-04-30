package edu.ds200.lab03.task2;

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

public class Task2GenreRatings {
  public static void main(String[] args) throws Exception {
    String labRoot = args.length > 0 ? args[0] : ".";
    Path outFile = Paths.get(args.length > 1 ? args[1] : "output/task2.txt");

    SparkConf conf = new SparkConf().setAppName("Lab03-Task2-GenreRatings");
    try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
      Path dataDir = Paths.get(labRoot, "Data");

      JavaRDD<String> movieLines = jsc.textFile(dataDir.resolve("movies.txt").toUri().toString());
      JavaPairRDD<Integer, String[]> movieGenres =
          movieLines.mapToPair(
              line -> {
                String[] p = line.split(",", 3);
                int id = Integer.parseInt(p[0].trim());
                String genres = p.length > 2 ? p[2].trim() : "";
                String[] gs = genres.isEmpty() ? new String[0] : genres.split("\\|");
                return new Tuple2<>(id, gs);
              });

      JavaRDD<String> r1 = jsc.textFile(dataDir.resolve("ratings_1.txt").toUri().toString());
      JavaRDD<String> r2 = jsc.textFile(dataDir.resolve("ratings_2.txt").toUri().toString());
      JavaPairRDD<Integer, Double> ratingByMovie =
          r1.union(r2)
              .mapToPair(
                  line -> {
                    String[] p = line.split(",");
                    int movieId = Integer.parseInt(p[1].trim());
                    double rating = Double.parseDouble(p[2].trim());
                    return new Tuple2<>(movieId, rating);
                  });

      JavaPairRDD<Integer, Tuple2<Double, String[]>> joined =
          ratingByMovie.join(movieGenres);

      JavaPairRDD<String, Tuple2<Double, Long>> genreSumCount =
          joined.flatMapToPair(
              t -> {
                double rating = t._2._1;
                String[] genres = t._2._2;
                List<Tuple2<String, Tuple2<Double, Long>>> pairs = new ArrayList<>();
                for (String g : genres) {
                  if (g != null && !g.isEmpty()) {
                    pairs.add(new Tuple2<>(g, new Tuple2<>(rating, 1L)));
                  }
                }
                return pairs.iterator();
              })
              .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

      List<Tuple2<String, Tuple2<Double, Long>>> rows = genreSumCount.collect();

      List<String> out = new ArrayList<>();
      out.add("=== Bài 2: Điểm trung bình theo thể loại ===");
      out.add("Genre\tAvgRating\tCount");
      rows.stream()
          .sorted(Comparator.comparing(Tuple2::_1))
          .forEach(
              row -> {
                double avg = row._2._1 / row._2._2;
                out.add(String.format("%s\t%s\t%d", row._1, RatingFormat.fourDecimals(avg), row._2._2));
              });

      IoUtil.writeLines(outFile, out);
    }
  }
}
