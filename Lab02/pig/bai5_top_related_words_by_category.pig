-- Bai 5:
-- Theo tung category, tim 5 tu lien quan nhat (tan so cao nhat)

%default INPUT 'data/hotel-review.csv'
%default STOPWORDS 'data/stopwords.txt'
%default OUTDIR 'output/bai5'

raw_reviews = LOAD '$INPUT' USING PigStorage(';')
    AS (rid:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '$STOPWORDS' USING PigStorage()
    AS (word:chararray);

normalized = FOREACH raw_reviews GENERATE
    category,
    LOWER(REPLACE(comment, '[^\\p{L}\\p{N}\\s]', ' ')) AS clean_comment;

tokens = FOREACH normalized GENERATE
    category,
    FLATTEN(TOKENIZE(clean_comment)) AS word;

filtered_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';
joined_stopwords = JOIN filtered_empty BY word LEFT OUTER, stopwords BY word;
clean_tokens = FILTER joined_stopwords BY stopwords::word IS NULL;
words = FOREACH clean_tokens GENERATE
    filtered_empty::category AS category,
    filtered_empty::word AS word;

grp_words = GROUP words BY (category, word);
word_count = FOREACH grp_words GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(words) AS freq;

-- Xep hang top 5 theo tung category bang self-join
wc_left = FOREACH word_count GENERATE category AS category, word AS word, freq AS freq;
wc_right = FOREACH word_count GENERATE category AS category, word AS word, freq AS freq;
wc_pairs = JOIN wc_left BY category, wc_right BY category;
wc_better_or_equal = FILTER wc_pairs BY
    (wc_right::freq > wc_left::freq) OR
    (wc_right::freq == wc_left::freq AND wc_right::word <= wc_left::word);
wc_rank_group = GROUP wc_better_or_equal BY (wc_left::category, wc_left::word, wc_left::freq);
wc_ranked = FOREACH wc_rank_group GENERATE
    FLATTEN(group) AS (category, word, freq),
    COUNT(wc_better_or_equal) AS rank_in_category;
top5_related = FILTER wc_ranked BY rank_in_category <= 5;
top5_related_sorted = ORDER top5_related BY category ASC, rank_in_category ASC, word ASC;

STORE top5_related_sorted INTO '$OUTDIR/top5_related_words_by_category' USING PigStorage('\t');
