--Author: Jiale Chen
--Name:Rejection of platform commodities
--DateTime:20220120
--Description:
--ModifyBy:
--ModifyDate:
--ModifyDesc:
--Copyright pinduoduo.com


----------------------------create table-----------------------------
#*
CREATE TABLE .
(

)
COMMENT ''
PARTITIONED BY (pt STRING COMMENT '日期分区') #The date of the partition--time distinction
STORED AS ORC
;
*#
------------------------------------------------------




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

-- 2.Temporary table processing


-- 3.summary
SELECT
 NVL(IF(t3.mall_id IS NOT NULL,'新商家','老商家'),'TOTAL') `商家类型`--New merchant ',' old merchant '),'TOTAL') 'merchant type'
,NVL(t1.fst_src,'TOTAL') `渠道`--TOTAL channel
,COUNT(t1.goods_id) `发布商品数`--Number of publisher goods
,COUNT(t4.goods_id) `动销商品数`--Number of moving pin goods
,COUNT(t4.goods_id)/COUNT(t1.goods_id) `动销率`--rate of moving pin
,SUM(IF(t1.fst_check_status=3,1,0)) `首次发布驳回商品数`--Number of rejected products for first release
,SUM(IF(t1.fst_check_status=3,1,0))/COUNT(t1.goods_id) `首次发布驳回率`--Rejection rate of first release
,SUM(IF(t1.fst_check_status=2,1,0)) `首次发布成功商品数`--Number of successful products released for the first time
,SUM(IF(t1.fst_check_status=2,1,0))/COUNT(t1.goods_id) `首次发布成功率`--Success rate of first release

,SUM(IF(t1.fst_check_status=3 AND t1.scnd_check_status=2,1,0)) `首次发布驳回后二次成功商品数`-- Number of second successful products after first release rejection
,SUM(IF(t1.fst_check_status=3 AND t1.scnd_check_status=2,1,0))/SUM(IF(t1.fst_check_status=3,1,0)) `首次发布驳回后二次成功商品占比`--Proportion of second successful products after first release rejection
,SUM(IF(t1.fst_check_status=3 AND t1.lst_check_status=2,1,0)) `首次发布驳回后最终成功商品数`--Final number of successful products after initial launch rejection
,SUM(IF(t1.fst_check_status=3 AND t1.lst_check_status=2,1,0))/SUM(IF(t1.fst_check_status=3,1,0)) `首次发布驳回后最终成功商品占比`--Percentage of final successful products after initial launch rejection
,SUM(IF(t1.fst_check_status=3 AND t1.lst_check_status IN (1,3),1,0)) `首次发布驳回后最终未成功商品数`--Number of products that failed after initial launch rejection
,SUM(IF(t1.fst_check_status=3 AND t1.lst_check_status IN (1,3),1,0))/SUM(IF(t1.fst_check_status=3,1,0)) `首次发布驳回后最终未成功商品占比`--Percentage of unsuccessful products after initial rejection

,AVG(IF(t1.fst_check_status=3 AND t1.lst_check_status=2,t1.lst_checked_time-t1.fst_sbmt_time,NULL)) `首次发布驳回后最终成功的平均等待时长`--Average waiting time after initial release rejection for final success
,AVG(IF(t1.fst_check_status=3 AND t1.lst_check_status=2,t1.cnt,NULL)) `首次发布驳回后最终成功的平均提交次数`--The average number of successful submissions after a first release rejection
FROM ( SELECT
        mall_id
       ,goods_id
       ,MIN(ARRAY(rn,src))[1] AS fst_src
       ,MIN(ARRAY(rn,is_shop))[1] AS fst_is_shop
       ,MIN(ARRAY(rn,check_status))[1] AS fst_check_status
       ,MAX(IF(rn=2,check_status,NULL)) AS scnd_check_status
       ,MAX(ARRAY(rn,check_status))[1] AS lst_check_status
       ,MIN(ARRAY(rn,sbmt_time))[1] AS fst_sbmt_time
       ,MAX(ARRAY(rn,checked_time))[1] AS lst_checked_time
       ,MAX(rn) AS cnt
       FROM ( SELECT
               ROW_NUMBER() OVER(PARTITION BY goods_id ORDER BY create_time ASC) AS rn
              ,mall_id
              ,goods_id
              ,CASE WHEN src = 0 THEN 'PC'
                    WHEN src = 1 THEN '开放平台'--open site
                    WHEN src = 2 THEN '商家APP'--B side APP
                    ELSE '其他'--others
                    END AS src
              ,check_status
              ,checked_time
              ,sbmt_time
              ,is_shop
              FROM dwd.dwd_prod_goods_cmmt_i_d
              WHERE pt BETWEEN '2021-11-01' AND '2021-12-01'
            ) m
       GROUP BY
        mall_id
       ,goods_id
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
LEFT JOIN ( SELECT
             mall_id
            FROM dwb.dwb_usr_mall_bsc_s_d
            WHERE pt = '${env.YYYYMMDD}'
            AND TO_DATE(fst_aprvd_time) >= '2021-10-01'
          ) t3
ON t1.mall_id = t3.mall_id
LEFT JOIN ( SELECT
             goods_id
            FROM dws.dws_trde_goods_std_d
            WHERE pt = '${env.YYYYMMDD}'
            AND ordr_crt_ordr_cnt_std > 0 -- 历史截至当日下单订单数>0
          ) t4
ON t1.goods_id = t4.goods_id
WHERE t2.mall_id IS NULL
AND t1.fst_src <> '其他'--others
AND t1.fst_is_shop = 0 -- 非线上商品
GROUP BY
 IF(t3.mall_id IS NOT NULL,'新商家','老商家')--new customers/old customers
,t1.fst_src
WITH CUBE
ORDER BY `商家类型`,`发布商品数` DESC--Merchant type ', 'number of publisher items
LIMIT 100
;

-- recycle
