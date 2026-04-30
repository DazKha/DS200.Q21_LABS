# Lab03 — Spark RDD (Java)

## Chuẩn bị

- **Java 11+**
- **`spark-submit`** để chạy bài: cần Spark đã cài (xem dưới)
- **`mvn`** *(khuyến nghị nếu chưa cài Spark)*: `./build.sh` dùng Maven tải thư viện Spark và biên dịch **không cần** `SPARK_HOME`

### Cài đặt Spark

**Homebrew (macOS):**

```bash
brew install apache-spark
export SPARK_HOME="$(brew --prefix apache-spark)/libexec"
```

(Apple Silicon thường là `/opt/homebrew/opt/apache-spark/libexec`; Intel có thể là `/usr/local/opt/apache-spark/libexec`.)

Thêm vào `~/.zshrc`: `export SPARK_HOME=...` rồi `source ~/.zshrc`.

**Cách 2 — Tải từ Apache:** vào https://spark.apache.org/downloads → chọn bản Pre-built → giải nén zip, ví dụ `~/apache-spark-3.5.x-bin-hadoop3`, rồi:

```bash
export SPARK_HOME="$HOME/apache-spark-3.5.x-bin-hadoop3"
```

Folder đó phải có thư mục con **`jars/`** và lệnh **`bin/spark-submit`**.

### Build khi chưa có Spark

```bash
brew install maven
./build.sh
```

Maven sẽ tải dependency Spark từ internet; chỉ riêng bước **chạy** (`scripts/task*.sh`) vẫn cần Spark đã cài hoặc `spark-submit` trong `PATH`.

## Biên dịch

```bash
cd Lab03
./build.sh
```

→ tạo `target/lab03-spark-rdd-1.0-SNAPSHOT.jar`

## Chạy từng bài

```bash
./scripts/task1/run.sh
# … tương tự task2 … task6
```

Hoặc chạy hết:

```bash
./scripts/run_all.sh
```

Kết quả: `output/task1.txt` … `output/task6.txt`

