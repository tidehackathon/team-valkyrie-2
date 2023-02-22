CREATE VIEW metabase.posts_with_features_view AS
SELECT p.*,
       lf.features AS lang_features,
       fv.bert_finetuned_propaganda_18,
       fv.distilroberta_propaganda_2class,
       fv.twitter_roberta_base_emotion,
       fv.twitter_roberta_base_sentiment_latest,
       fv.fake_news_bert_detect,
       c.score AS claimbuster_score
       FROM metabase.posts p
    LEFT JOIN metabase.linguistic_features lf
        ON p.original_id = lf.post_id
    LEFT JOIN metabase.features_v1 fv
        ON fv.original_id = p.original_id
    LEFT JOIN metabase.claimbuster c
        ON p.id = c.post_id;
