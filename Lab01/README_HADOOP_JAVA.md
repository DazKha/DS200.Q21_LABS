# Hadoop Java solutions for Lab01

## Files

- `MovieRatingStatsJob.java` (Bai 1)
- `GenreRatingAnalysisJob.java` (Bai 2)
- `GenderRatingByMovieJob.java` (Bai 3)
- `AgeGroupRatingByMovieJob.java` (Bai 4)

## Compile

Set Hadoop classpath and compile:

```bash
cd /Users/minhkha/Desktop/DS200/Lab01
rm -rf scripts/build scripts/lab01-jobs.jar outputs
mkdir -p outputs

export HADOOP_CLASSPATH=$(hadoop classpath)
javac --release 11 -classpath "$HADOOP_CLASSPATH" -d scripts/build scripts/*.java
jar -cvf scripts/lab01-jobs.jar -C scripts/build/ .
```

## Run examples

```bash
hadoop jar scripts/lab01-jobs.jar MovieRatingStatsJob data/ratings_1.txt data/ratings_2.txt data/movies.txt outputs/output_bai1
hadoop jar scripts/lab01-jobs.jar GenreRatingAnalysisJob data/ratings_1.txt data/ratings_2.txt data/movies.txt outputs/output_bai2
hadoop jar scripts/lab01-jobs.jar GenderRatingByMovieJob data/ratings_1.txt data/ratings_2.txt data/users.txt data/movies.txt outputs/output_bai3
hadoop jar scripts/lab01-jobs.jar AgeGroupRatingByMovieJob data/ratings_1.txt data/ratings_2.txt data/users.txt data/movies.txt outputs/output_bai4
```

## Notes

- Each job reads both `ratings_1.txt` and `ratings_2.txt`.
- Jobs that need lookup data use distributed cache (`movies.txt`, `users.txt`).
- Output format is aligned with assignment requirements.
- Output lines are sorted alphabetically before final write.
- Each output folder contains:
  - `part-r-00000` (sorted result)
  - `bai1_result.txt`, `bai2_result.txt`, `bai3_result.txt`, or `bai4_result.txt`
  - `_SUCCESS`
