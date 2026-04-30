# DS200 Lab02 - Apache Pig - Le Minh Kha - 23520664

Repo nay gom cac script Apache Pig cho 5 bai trong `assignments.ipynb`.

## Cau truc thu muc

- `data/hotel-review.csv`: du lieu dau vao
- `data/stopwords.txt`: danh sach stopword
- `pig/*.pig`: script cho tung bai
- `run_all.sh`: chay tu dong tat ca bai
- `output/`: ket qua sau khi chay

## Yeu cau moi truong

- Da cai dat Apache Pig
- Co the chay tren:
  - `local` mode (thu nghiem tren may ca nhan)
  - `mapreduce` mode (Hadoop cluster)

Kiem tra Pig:

```bash
pig -version
```

## Chay tung bai

### Local mode

```bash
pig -x local pig/bai1_preprocess.pig
pig -x local pig/bai2_statistics.pig
pig -x local pig/bai3_best_worst_aspect.pig
pig -x local pig/bai4_top_sentiment_words_by_category.pig
pig -x local pig/bai5_top_related_words_by_category.pig
```

### MapReduce mode

```bash
pig -x mapreduce pig/bai1_preprocess.pig
pig -x mapreduce pig/bai2_statistics.pig
pig -x mapreduce pig/bai3_best_worst_aspect.pig
pig -x mapreduce pig/bai4_top_sentiment_words_by_category.pig
pig -x mapreduce pig/bai5_top_related_words_by_category.pig
```

## Chay tu dong tat ca bai

Script `run_all.sh` se:

- xoa output cu (`output/bai1` ... `output/bai5`)
- chay lan luot 5 script Pig
- tao them file `.csv` cho tung ket qua con (tu cac `part-*`)
- tao file tong hop `.txt` trong tung thu muc `output/baiX` ngay sau moi bai chay xong
- dung ngay neu co loi

### Cach dung

Mac dinh chay `mapreduce`:

```bash
./run_all.sh
```

Chi dinh mode:

```bash
./run_all.sh local
./run_all.sh mapreduce
```

Neu can, ban co the set bien moi truong `PIG_BIN`:

```bash
PIG_BIN=/duong/dan/toi/pig ./run_all.sh mapreduce
```

## File `.txt` de chup man hinh

Sau khi chay `run_all.sh`, moi bai se co them 1 file tong hop:

- `output/bai1/ket_qua_bai1.txt`
- `output/bai2/ket_qua_bai2.txt`
- `output/bai3/ket_qua_bai3.txt`
- `output/bai4/ket_qua_bai4.txt`
- `output/bai5/ket_qua_bai5.txt`

Moi file la ban tom tat de chup man hinh, gom:

- thong tin user/host/thoi gian chay
- mode chay (`local` hoac `mapreduce`)
- ket qua cua tung file `part-*` (hien thi 40 dong dau)

Neu bai nao khong sinh ra `part-*`, file txt se ghi ro de ban debug.

## File `.csv` bo sung

Sau khi chay `run_all.sh`, trong moi `output/baiX` se co them cac file `.csv` theo tung nhom ket qua.
Vi du:

- `output/bai1/clean_words.csv`
- `output/bai1/normalized_comments.csv`
- `output/bai2/review_count_by_aspect.csv`

Noi dung CSV duoc tao bang cach gop cac `part-*` trong tung thu muc ket qua.
