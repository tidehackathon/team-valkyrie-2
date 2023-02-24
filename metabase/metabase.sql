--Disinformation posts by period
SELECT
    s.name as "name"
    ,sum(s.cnt) as "number of posts"
    ,s.dates as by_period
FROM
(
SELECT sources.name
, count(*) as cnt
, DATE_TRUNC({{date_part}},posts.original_timestamp) as dates
    from metabase.posts
    join metabase.sources
      on posts.source_id=sources.id
    WHERE true
    [[and {{original_timestamp}}]]
    [[and {{source_name}}]]
    [[and sources.rating<={{post_rating}}]]
group by
  sources.name
  , posts.original_timestamp
  , DATE_TRUNC({{date_part}},posts.original_timestamp)
  ) s
group by s.name, s.dates
order by s.dates desc
--Post Rating
SELECT distinct {{rating}} as Rating
from metabase.posts
--Posts by period
SELECT
    sum(s.disinformation) as "disinformation"
    ,sum(s.normal_post) as "normal_post"
    ,s.dates
FROM
(
SELECT sources.name
, case
     when posts.rating < {{posts_rating}}
     then 1
     else 0
  end as disinformation
,  case
     when posts.rating >= {{posts_rating}}
     then 1
     else 0
  end as normal_post
, DATE_TRUNC({{date_part}},posts.original_timestamp) as dates
    from metabase.posts
    join metabase.sources
      on posts.source_id=sources.id
    WHERE true
    [[and {{original_timestamp}}]]
    [[and {{source_name}}]]
  ) s
group by s.name, s.dates
order by s.name,  s.dates
--Top posts
SELECT DISTINCT
source_id
    ,source_name as User
    ,url
    ,content
    ,content_rendered
    ,replace(replace(external_urls::text,'["',''),'"]','') as external_urls
    ,reply_count
    ,share_count
    ,like_count
    ,quote_count
    ,replace(replace(replace(hashtags::text,'[',''),']',''),'"','') as hashtags
    ,rating as Rating
    ,original_timestamp as post_date
    , 'DETAILS' as details
from metabase.posts
WHERE true
[[and DATE_TRUNC({{date_part}},posts.original_timestamp) = {{original_timestamp_trunk}}::timestamp]]
[[and {{original_timestamp}}]]
[[and {{source_name}}]]
[[and rating<{{post_rating}}]]
order by posts.original_timestamp desc
limit {{top}}
--Top users with low rating
with src as (
    Select sources.name, posts.original_timestamp,sources.rating
    from metabase.posts
    join metabase.sources
      on posts.source_id=sources.id
    WHERE true
    [[and {{original_timestamp}}]]
    [[and {{source_name}}]]
    [[and sources.rating<{{rating}}]]
    )
SELECT
    s.name
    , date_trunc('hour',s.original_timestamp)
    , count(s.name) as cnt
FROM (
    Select src.name
    from src
    group by src.name
    order by count(src.name) desc
    limit {{top}}
) top
JOIN src s
ON top.name=s.name
GROUP BY s.name, date_trunc('hour',s.original_timestamp)
ORDER by date_trunc('hour',s.original_timestamp) desc
--Top users with low rating: table
with src as (
    Select sources.name, posts.original_timestamp, sources.rating
    from metabase.posts
    join metabase.sources
      on posts.source_id=sources.id
    WHERE true
    [[and {{original_timestamp}}]]
    [[and sources.rating<{{rating}}]]
    )
SELECT
    s.name as User
    , count(s.name) as Posts
    , s.rating as Rating
FROM (
    Select src.name
    from src
    group by src.name
    order by count(src.name) desc
    limit {{top}}
) top
JOIN src s
ON top.name=s.name
GROUP BY s.name, s.rating
order by s.rating, s.name
--User detail
SELECT distinct name as Name
, LEFT(url, POSITION(name IN url)+length(name)-1) as URL
,rating
from metabase.sources
where true
[[and {{source_name}}]]
[[and rating<{{rating}}]]
--User Rating
SELECT rating as Rating
from metabase.sources
where {{source_name}}