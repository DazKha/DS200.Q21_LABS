-- Bai 2:
-- 1) Thong ke tan so tu, loc tu xuat hien > 500 lan
-- 2) Thong ke so binh luan theo category
-- 3) Thong ke so binh luan theo aspect

%default INPUT 'data/hotel-review.csv'
%default STOPWORDS 'data/stopwords.txt'
%default OUTDIR 'output/bai2'

raw_reviews = LOAD '$INPUT' USING PigStorage(';')
    AS (rid:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '$STOPWORDS' USING PigStorage()
    AS (word:chararray);

normalized = FOREACH raw_reviews GENERATE
    rid,
    LOWER(REPLACE(comment, '[^\\p{L}\\p{N}\\s]', ' ')) AS clean_comment,
    category,
    aspect,
    sentiment;

tokens = FOREACH normalized GENERATE
    rid,
    category,
    aspect,
    FLATTEN(TOKENIZE(clean_comment)) AS word;

filtered_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';
joined_stopwords = JOIN filtered_empty BY word LEFT OUTER, stopwords BY word;
clean_words = FILTER joined_stopwords BY stopwords::word IS NULL;
words = FOREACH clean_words GENERATE filtered_empty::word AS word;

grp_words = GROUP words BY word;
word_freq = FOREACH grp_words GENERATE group AS word, COUNT(words) AS freq;
word_freq_500 = FILTER word_freq BY freq > 500;
word_freq_500_sorted = ORDER word_freq_500 BY freq DESC;

grp_category = GROUP raw_reviews BY category;
count_by_category = FOREACH grp_category GENERATE group AS category, COUNT(raw_reviews) AS review_count;
count_by_category_sorted = ORDER count_by_category BY review_count DESC;

grp_aspect = GROUP raw_reviews BY aspect;
count_by_aspect = FOREACH grp_aspect GENERATE group AS aspect, COUNT(raw_reviews) AS review_count;
count_by_aspect_sorted = ORDER count_by_aspect BY review_count DESC;

STORE word_freq_500_sorted INTO '$OUTDIR/word_freq_gt_500' USING PigStorage('\t');
STORE count_by_category_sorted INTO '$OUTDIR/review_count_by_category' USING PigStorage('\t');
STORE count_by_aspect_sorted INTO '$OUTDIR/review_count_by_aspect' USING PigStorage('\t');
