-- Bai 1: Lowercase, tach tu, loai bo stopwords

%default INPUT 'data/hotel-review.csv'
%default STOPWORDS 'data/stopwords.txt'
%default OUTDIR 'output/bai1'

raw_reviews = LOAD '$INPUT' USING PigStorage(';')
    AS (rid:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '$STOPWORDS' USING PigStorage()
    AS (word:chararray);

-- Chuan hoa chu thuong va thay dau cau bang khoang trang
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
    sentiment,
    FLATTEN(TOKENIZE(clean_comment)) AS word;

filtered_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';

filtered_words = JOIN filtered_empty BY word LEFT OUTER, stopwords BY word;
clean_words = FILTER filtered_words BY stopwords::word IS NULL;
final_words = FOREACH clean_words GENERATE
    filtered_empty::rid AS rid,
    filtered_empty::category AS category,
    filtered_empty::aspect AS aspect,
    filtered_empty::sentiment AS sentiment,
    filtered_empty::word AS word;

STORE final_words INTO '$OUTDIR/clean_words' USING PigStorage('\t');
STORE normalized INTO '$OUTDIR/normalized_comments' USING PigStorage('\t');
