-- Bai 4:
-- Theo tung category:
-- 1) Tim 5 tu tich cuc nhat
-- 2) Tim 5 tu tieu cuc nhat

%default INPUT 'data/hotel-review.csv'
%default STOPWORDS 'data/stopwords.txt'
%default OUTDIR 'output/bai4'

raw_reviews = LOAD '$INPUT' USING PigStorage(';')
    AS (rid:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '$STOPWORDS' USING PigStorage()
    AS (word:chararray);

normalized = FOREACH raw_reviews GENERATE
    rid,
    category,
    sentiment,
    LOWER(REPLACE(comment, '[^\\p{L}\\p{N}\\s]', ' ')) AS clean_comment;

tokens = FOREACH normalized GENERATE
    category,
    sentiment,
    FLATTEN(TOKENIZE(clean_comment)) AS word;

filtered_empty = FILTER tokens BY word IS NOT NULL AND TRIM(word) != '';
joined_stopwords = JOIN filtered_empty BY word LEFT OUTER, stopwords BY word;
clean_tokens = FILTER joined_stopwords BY stopwords::word IS NULL;
words = FOREACH clean_tokens GENERATE
    filtered_empty::category AS category,
    filtered_empty::sentiment AS sentiment,
    filtered_empty::word AS word;

positive_words = FILTER words BY sentiment == 'positive';
negative_words = FILTER words BY sentiment == 'negative';

grp_pos_word = GROUP positive_words BY (category, word);
pos_word_count = FOREACH grp_pos_word GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(positive_words) AS freq;

grp_neg_word = GROUP negative_words BY (category, word);
neg_word_count = FOREACH grp_neg_word GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(negative_words) AS freq;

-- Xep hang top 5 theo tung category bang self-join
-- rank = so tu trong cung category co muc uu tien >= tu dang xet
pos_left = FOREACH pos_word_count GENERATE category AS category, word AS word, freq AS freq;
pos_right = FOREACH pos_word_count GENERATE category AS category, word AS word, freq AS freq;
pos_pairs = JOIN pos_left BY category, pos_right BY category;
pos_better_or_equal = FILTER pos_pairs BY
    (pos_right::freq > pos_left::freq) OR
    (pos_right::freq == pos_left::freq AND pos_right::word <= pos_left::word);
pos_rank_group = GROUP pos_better_or_equal BY (pos_left::category, pos_left::word, pos_left::freq);
pos_ranked = FOREACH pos_rank_group GENERATE
    FLATTEN(group) AS (category, word, freq),
    COUNT(pos_better_or_equal) AS rank_in_category;
top5_pos = FILTER pos_ranked BY rank_in_category <= 5;
top5_pos_sorted = ORDER top5_pos BY category ASC, rank_in_category ASC, word ASC;

neg_left = FOREACH neg_word_count GENERATE category AS category, word AS word, freq AS freq;
neg_right = FOREACH neg_word_count GENERATE category AS category, word AS word, freq AS freq;
neg_pairs = JOIN neg_left BY category, neg_right BY category;
neg_better_or_equal = FILTER neg_pairs BY
    (neg_right::freq > neg_left::freq) OR
    (neg_right::freq == neg_left::freq AND neg_right::word <= neg_left::word);
neg_rank_group = GROUP neg_better_or_equal BY (neg_left::category, neg_left::word, neg_left::freq);
neg_ranked = FOREACH neg_rank_group GENERATE
    FLATTEN(group) AS (category, word, freq),
    COUNT(neg_better_or_equal) AS rank_in_category;
top5_neg = FILTER neg_ranked BY rank_in_category <= 5;
top5_neg_sorted = ORDER top5_neg BY category ASC, rank_in_category ASC, word ASC;

STORE top5_pos_sorted INTO '$OUTDIR/top5_positive_words_by_category' USING PigStorage('\t');
STORE top5_neg_sorted INTO '$OUTDIR/top5_negative_words_by_category' USING PigStorage('\t');
