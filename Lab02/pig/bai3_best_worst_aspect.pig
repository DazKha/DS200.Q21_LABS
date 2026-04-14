-- Bai 3:
-- Tim aspect co so danh gia negative nhieu nhat
-- va aspect co so danh gia positive nhieu nhat

%default INPUT 'data/hotel-review.csv'
%default OUTDIR 'output/bai3'

raw_reviews = LOAD '$INPUT' USING PigStorage(';')
    AS (rid:int, comment:chararray, category:chararray, aspect:chararray, sentiment:chararray);

positive_reviews = FILTER raw_reviews BY sentiment == 'positive';
negative_reviews = FILTER raw_reviews BY sentiment == 'negative';

grp_pos = GROUP positive_reviews BY aspect;
count_pos = FOREACH grp_pos GENERATE group AS aspect, COUNT(positive_reviews) AS positive_count;
grp_pos_all = GROUP count_pos ALL;
max_pos = FOREACH grp_pos_all GENERATE MAX(count_pos.positive_count) AS max_positive_count;
top_positive_join = JOIN count_pos BY positive_count, max_pos BY max_positive_count;
top_positive_aspect = FOREACH top_positive_join GENERATE
    count_pos::aspect AS aspect,
    count_pos::positive_count AS positive_count;

grp_neg = GROUP negative_reviews BY aspect;
count_neg = FOREACH grp_neg GENERATE group AS aspect, COUNT(negative_reviews) AS negative_count;
grp_neg_all = GROUP count_neg ALL;
max_neg = FOREACH grp_neg_all GENERATE MAX(count_neg.negative_count) AS max_negative_count;
top_negative_join = JOIN count_neg BY negative_count, max_neg BY max_negative_count;
top_negative_aspect = FOREACH top_negative_join GENERATE
    count_neg::aspect AS aspect,
    count_neg::negative_count AS negative_count;

STORE top_positive_aspect INTO '$OUTDIR/top_positive_aspect' USING PigStorage('\t');
STORE top_negative_aspect INTO '$OUTDIR/top_negative_aspect' USING PigStorage('\t');

STORE count_pos INTO '$OUTDIR/all_positive_aspect_counts' USING PigStorage('\t');
STORE count_neg INTO '$OUTDIR/all_negative_aspect_counts' USING PigStorage('\t');
