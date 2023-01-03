--Author: Jiale Chen
--Name:Analysis of reasons for low secondary submission rate after rejection of the first release of goods
--DateTime:20220126
--Description:
--ModifyBy:
--ModifyDate:
--ModifyDesc:
--Copyright pinduoduo.com


-----------------------------create table-----------------------------
#*
CREATE TABLE .
(

)
COMMENT ''
PARTITIONED BY (pt STRING COMMENT )
STORED AS ORC
;
*#


----------------------------SQL processing---------------------------
-- 1.Parameter setting
-- SET tez.grouping.min-size=1024000000;
-- SET tez.grouping.max-size=2048000000;
-- SET mapred.max.split.size=2048000000;
-- SET mapred.min.split.size=2048000000;
-- SET mapreduce.job.reduces=3000;
-- SET hive.tez.container.size=4000;
SET hive.exec.orc.split.strategy=BI;
SET tez.grouping.split-waves=0;
SET hive.map.aggr = true;
SET hive.groupby.skewindata=true;

-- 2.table processing
DROP TABLE IF EXISTS jialechen.gauss_85942_${env.YYYYMMDD8}_1;
CREATE TABLE jialechen.gauss_85942_${env.YYYYMMDD8}_1 AS
SELECT
 t1.*
FROM ( SELECT
        pt
       ,ROW_NUMBER() OVER(PARTITION BY goods_id ORDER BY create_time ASC) AS rn
       ,mall_id
       ,goods_id
       ,goods_name
       ,cate_id
       ,CASE WHEN src = 0 THEN 'PC'
             WHEN src = 1 THEN '开放平台'--open site
             WHEN src = 2 THEN '商家APP'--platform APP
             ELSE '其他'
             END AS src
       ,check_status
       ,checked_time
       ,sbmt_time
       ,create_time
       ,is_shop
       FROM dwd.dwd_prod_goods_cmmt_i_d
       WHERE pt BETWEEN '2021-11-01' AND '2021-12-01'
     ) t1
LEFT JOIN ( SELECT
             mall_id
            FROM goods.goods_oprty_gojp_mall_s_d
            WHERE pt = '${env.YYYYMMDD}'
            AND is_blk = 0
            AND is_prom = 0
            GROUP BY mall_id
          ) t2
ON t1.mall_id = t2.mall_id
WHERE t2.mall_id IS NULL
;

-- 3.summary
-- 1
SELECT
 NVL(fst_src,'TOTAL') `渠道`--channel
,SUM(1) `首次发布驳回商品数`--Number of rejected products for first release
,SUM(IF(cnt>=2,1,0)) `首次发布驳回并再次提交商品数`--First release rejected and resubmitted item count
FROM ( SELECT
        mall_id
       ,goods_id
       ,MIN(ARRAY(rn,src))[1] AS fst_src -- 首次的渠道
       ,MIN(ARRAY(rn,is_shop))[1] AS fst_is_shop -- 首次是否是线上商品
       ,MIN(ARRAY(rn,check_status))[1] AS fst_check_status -- 首次的审核状态
       ,MAX(rn) AS cnt
       FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
       GROUP BY
        mall_id
       ,goods_id
     ) t
WHERE fst_src <> '其他'--others
AND fst_is_shop = 0
AND fst_check_status = 3
GROUP BY fst_src
WITH CUBE
ORDER BY `渠道`--channels
LIMIT 100
;
SELECT
 fst_src `渠道`--channel
,mall_id `店铺ID`--store ID
,goods_id `商品ID`--good ID
,fst_goods_name `首次发布的商品名称`--name for the publisher
FROM ( SELECT
        fst_src
       ,mall_id
       ,goods_id
       ,fst_goods_name
       ,ROW_NUMBER() OVER(PARTITION BY fst_src ORDER BY RAND()) AS rn
       FROM ( SELECT
               mall_id
              ,goods_id
              ,MIN(ARRAY(rn,goods_name))[1] AS fst_goods_name
              ,MIN(ARRAY(rn,src))[1] AS fst_src
              ,MIN(ARRAY(rn,is_shop))[1] AS fst_is_shop
              ,MIN(ARRAY(rn,check_status))[1] AS fst_check_status
              ,MAX(rn) AS cnt
              FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
              GROUP BY
               mall_id
              ,goods_id
            ) m
       WHERE fst_src <> '其他'--others
       AND fst_is_shop = 0
       AND fst_check_status = 3
       AND cnt = 1
     ) t
WHERE rn <= 250
ORDER BY `渠道`,`商品ID`--channel, good ID
LIMIT 100000
;


-- 2
SELECT
 NVL(t1.fst_src,'TOTAL') `渠道`--channel
,SUM(1) `首次发布驳回后最终未成功商品数`--Number of products that failed after initial launch rejection
,SUM(IF(t2.cate_id IS NULL,1,0)) `首次发布驳回后最终未成功,且店铺内无该叶子类目商品数`--The first release was rejected and failed, and there was no product number
FROM ( SELECT
        *
       FROM ( SELECT
               mall_id
              ,goods_id
              ,MIN(ARRAY(rn,pt))[1] AS fst_pt
              ,MIN(ARRAY(rn,cate_id))[1] AS fst_cate_id
              ,MIN(ARRAY(rn,src))[1] AS fst_src
              ,MIN(ARRAY(rn,is_shop))[1] AS fst_is_shop
              ,MIN(ARRAY(rn,check_status))[1] AS fst_check_status
              ,MAX(ARRAY(rn,check_status))[1] AS lst_check_status
              ,MAX(rn) AS cnt
              FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
              GROUP BY
               mall_id
              ,goods_id
            ) m
       WHERE fst_pt = '2021-11-01'
       AND fst_src <> '其他'--others
       AND fst_is_shop = 0 -- 非线上商品
       AND fst_check_status = 3 -- 首次发布驳回
       AND lst_check_status IN (1,3) -- 末次发布未成功
     ) t1
LEFT JOIN ( SELECT
             mall_id
            ,cate_id
            FROM dwb.dwb_prod_goods_info_s_d
            WHERE pt = '2021-10-31'
            AND is_avlb_buy = 1
            GROUP BY
             mall_id
            ,cate_id
          ) t2
ON t1.mall_id = t2.mall_id
AND t1.fst_cate_id = t2.cate_id
GROUP BY t1.fst_src
WITH CUBE
ORDER BY `渠道`--channel
LIMIT 100
;


-- 3
SELECT
 NVL(t1.fst_src,'TOTAL') `渠道`--Total channel
,COUNT(DISTINCT t1.mall_id) `首次发布驳回且未再次提交店铺数`--The first release was rejected and the number of stores was not submitted again
,COUNT(DISTINCT IF(t2.cate_id IS NOT NULL AND t2.sbmt_time>t1.fst_checked_time,t1.mall_id,NULL)) `首次发布驳回且未再次提交店铺中，3天内发布同叶子类目商品的店铺数`
--Number of stores that rejected the first release and did not submit the product again within 3 days
,COUNT(DISTINCT IF(t3.goods_name IS NOT NULL AND t2.sbmt_time>t1.fst_checked_time,t1.mall_id,NULL)) `首次发布驳回且未再次提交店铺中，3天内发布同标题商品的店铺数`
--Number of stores that rejected the first release and did not submit the product again within 3 days
FROM ( SELECT
        *
       FROM ( SELECT
               *
              ,ROW_NUMBER() OVER(PARTITION BY mall_id ORDER BY fst_checked_time ASC) AS rn
              FROM ( SELECT
                      mall_id
                     ,goods_id
                     ,MIN(ARRAY(rn,pt))[1] AS fst_pt
                     ,MIN(ARRAY(rn,checked_time))[1] AS fst_checked_time
                     ,MIN(ARRAY(rn,cate_id))[1] AS fst_cate_id
                     ,MIN(ARRAY(rn,goods_name))[1] AS fst_goods_name
                     ,MIN(ARRAY(rn,src))[1] AS fst_src
                     ,MIN(ARRAY(rn,is_shop))[1] AS fst_is_shop
                     ,MIN(ARRAY(rn,check_status))[1] AS fst_check_status
                     ,MAX(rn) AS cnt
                     FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
                     GROUP BY
                      mall_id
                     ,goods_id
                   ) n
              WHERE fst_pt = '2021-11-01'
              AND TO_DATE(from_unixtime(fst_checked_time,'yyyy-MM-dd HH:mm:ss')) = '2021-11-01'
              AND fst_src <> '其他'
              AND fst_is_shop = 0
              AND fst_check_status = 3
              AND cnt = 1
            ) m
       WHERE rn = 1
     ) t1
LEFT JOIN ( SELECT
             mall_id
            ,cate_id
            ,sbmt_time
            FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
            WHERE is_shop = 0
            AND TO_DATE(from_unixtime(sbmt_time,'yyyy-MM-dd HH:mm:ss')) BETWEEN '2021-11-01' AND '2021-11-03'
          ) t2
ON t1.mall_id = t2.mall_id
AND t1.fst_cate_id = t2.cate_id
LEFT JOIN ( SELECT
             mall_id
            ,goods_name
            ,sbmt_time
            FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
            WHERE is_shop = 0
            AND TO_DATE(from_unixtime(sbmt_time,'yyyy-MM-dd HH:mm:ss')) BETWEEN '2021-11-01' AND '2021-11-03'
          ) t3
ON t1.mall_id = t3.mall_id
AND t1.fst_goods_name = t3.goods_name
GROUP BY t1.fst_src
WITH CUBE
ORDER BY `渠道`--channel
LIMIT 100
;

-- 4
SELECT
 src `渠道`--channel
,mall_id `店铺ID`--store name
,rjt_goods_id `首次发布驳回且未再次提交的商品ID`--item ID rejected for the first time and not resubmitted
,rjt_goods_name `驳回商品的名称`--name of the rejected good
,rjt_checked_time `审核时间`--time for inspection
,new_goods_id `同店驳回后新提交的商品ID`--New product ID submitted after same-store rejection
,new_goods_name `提交商品的名称`--Submit the name of the commodity
,new_sbmt_time `提交时间`--time for submission
FROM ( SELECT
        *
       ,ROW_NUMBER() OVER(PARTITION BY src,mall_id ORDER BY new_sbmt_time ASC) AS rn
       FROM ( SELECT
               t1.fst_src AS src
              ,t1.mall_id
              ,from_unixtime(t1.fst_checked_time,'yyyy-MM-dd HH:mm:ss') AS rjt_checked_time
              ,t1.goods_id AS rjt_goods_id
              ,t1.fst_goods_name AS rjt_goods_name
              ,from_unixtime(t2.sbmt_time,'yyyy-MM-dd HH:mm:ss') AS new_sbmt_time
              ,t2.goods_id AS new_goods_id
              ,t2.goods_name AS new_goods_name
              FROM ( SELECT
                      *
                     FROM ( SELECT
                             *
                            ,ROW_NUMBER() OVER(PARTITION BY fst_src ORDER BY RAND()) AS rn_2
                            FROM ( SELECT
                                    *
                                   FROM ( SELECT
                                           *
                                          ,ROW_NUMBER() OVER(PARTITION BY mall_id ORDER BY fst_checked_time ASC) AS rn_1
                                          FROM ( SELECT
                                                  mall_id
                                                 ,goods_id
                                                 ,MIN(ARRAY(rn,pt))[1] AS fst_pt
                                                 ,MIN(ARRAY(rn,checked_time))[1] AS fst_checked_time
                                                 ,MIN(ARRAY(rn,cate_id))[1] AS fst_cate_id
                                                 ,MIN(ARRAY(rn,goods_name))[1] AS fst_goods_name
                                                 ,MIN(ARRAY(rn,src))[1] AS fst_src
                                                 ,MIN(ARRAY(rn,is_shop))[1] AS fst_is_shop
                                                 ,MIN(ARRAY(rn,check_status))[1] AS fst_check_status
                                                 ,MAX(rn) AS cnt
                                                 FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
                                                 GROUP BY
                                                  mall_id
                                                 ,goods_id
                                               ) nn
                                          WHERE fst_pt = '2021-11-01'
                                          AND TO_DATE(from_unixtime(fst_checked_time,'yyyy-MM-dd HH:mm:ss')) = '2021-11-01'
                                          AND fst_src <> '其他'--others
                                          AND fst_is_shop = 0 -- 非线上商品
                                          AND fst_check_status = 3
                                          AND cnt = 1
                                        ) n
                                   WHERE rn_1 = 1
                                 ) mm
                          ) m
                     WHERE rn_2 <= 250
                   ) t1
              LEFT JOIN ( SELECT
                           mall_id
                          ,goods_id
                          ,goods_name
                          ,sbmt_time
                          FROM jialechen.gauss_85942_${env.YYYYMMDD8}_1
                          WHERE is_shop = 0
                        ) t2
              ON t1.mall_id = t2.mall_id
              WHERE t2.mall_id IS NULL
              OR (t2.mall_id IS NOT NULL AND t2.sbmt_time > t1.fst_checked_time)
              GROUP BY
               t1.fst_src
              ,t1.mall_id
              ,from_unixtime(t1.fst_checked_time,'yyyy-MM-dd HH:mm:ss')
              ,t1.goods_id
              ,t1.fst_goods_name
              ,from_unixtime(t2.sbmt_time,'yyyy-MM-dd HH:mm:ss')
              ,t2.goods_id
              ,t2.goods_name
            ) tt
     ) t
WHERE rn <= 10
ORDER BY `渠道`,`店铺ID`,`提交时间`--'channel', 'store ID', 'submission time'
LIMIT 100000
;

-- 4.recycle
DROP TABLE IF EXISTS jialechen.gauss_85942_${env.YYYYMMDD8}_1;
