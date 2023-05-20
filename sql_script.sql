/* создание схемы `schema_trade`/ creation of schema `schema_trade` */
CREATE SCHEMA schema_trade;

/* создание таблицы `trade`: id, регион, тип торговли, дата транзакции, категория товаров, оборот, средний чек /
/ creation of table `trade`: id, region, trade type, date of transaction, category of goods, turnover, average check*/
CREATE TABLE trade (
    id INT PRIMARY KEY AUTO_INCREMENT,
	region VARCHAR(40),
	type_trade VARCHAR(30),
	date_trade DATE,
	category VARCHAR(40),
	turnover DECIMAL(8, 2),
	avg_check DECIMAL(8, 2)
);

/* ввод значений из файла csv в таблицу `trade` с помощью мастера импорта табличных данных/
import values from file csv into table `trade` through Table Data Import Wizard */

/* выборка первых 10-ти строк таблицы `trade`/
selection of the first 10 rows of the `trade`*/
SELECT *
  FROM trade
 LIMIT 10;

/* выборка и сравнение оборота (total_turnover) и среднего чека (avg_check) по аналогичным кварталам в 2022 и 2021 годах (yoy)/
selection and comparison of turnover and average check by comparable quarters in 2022 and 2021 years (yoy)*/
WITH t1 AS (
    SELECT QUARTER(date_trade) AS "quarter_trade",
           YEAR(date_trade) AS "year_trade",
	       ROUND(SUM(turnover), 0) AS "total_turnover",
           ROUND(AVG(avg_check), 0) AS "avg_check"
      FROM trade
     GROUP BY QUARTER(date_trade), YEAR(date_trade)
)
SELECT year_trade,
       quarter_trade,
       total_turnover,
       ROUND((total_turnover - LAG(total_turnover, 4) OVER (
             ORDER BY year_trade, quarter_trade)) / LAG(total_turnover, 4) OVER (
             ORDER BY year_trade, quarter_trade)*100, 0) AS "yoy_turnover_percentage",
       avg_check,
       ROUND((avg_check - LAG(avg_check, 4) OVER (
             ORDER BY year_trade, quarter_trade)) / LAG(avg_check, 4) OVER (
             ORDER BY year_trade, quarter_trade)*100, 0) AS "yoy_avg_check_percentage"
  FROM t1
  ORDER BY year_trade Desc, quarter_trade Asc;

/* создание представления 'trade_region'/
creation of view 'trade_region'*/
CREATE VIEW trade_region
AS SELECT *
     FROM (
      WITH t2 AS (
           SELECT YEAR(date_trade) AS "year_trade",
                  region,
	              ROUND(SUM(turnover), 0) AS "total_turnover",
                  ROUND(AVG(avg_check), 0) AS "avg_check"
             FROM trade
            GROUP BY region, YEAR(date_trade)
)
SELECT region,
       year_trade,
       total_turnover,	
       ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage",
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
  FROM t2
 ORDER BY year_trade Desc,
          total_turnover Desc,
          region) AS t3;

/* выборка и сравнение оборота (total_turnover) и среднего чека (avg_check) по регионам в 2022 и 2021 годах (yoy) из представления 'trade_region'/
selection and comparison of turnover (total_turnover) and average check (avg_check) by regions in 2022 and 2021 years (yoy) from view 'trade_region'*/
SELECT region,
       year_trade,
       total_turnover,	
       ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage",
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
  FROM trade_region
 ORDER BY year_trade Desc,
          total_turnover Desc,
          region;

/* выборка оборота (total_turnover), выборка доли оборота (turnover_percentage) и
сравнение доли оборота (yoy_turnover_percentage) по регионам в 2022 и 2021 годах/
selection of turnover (total_turnover), selection of turnover share (turnover_percentage) and
comparison of turnover share (yoy_turnover_percentage) by regions in 2022 and 2021 years */
SELECT year_trade,
	   region,
	   total_turnover,
       turnover_percentage,
       ROUND((turnover_percentage - LAG(turnover_percentage, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)), 2) AS "yoy_turnover_percentage"
  FROM (
         SELECT year_trade,
	            region,
	            ROUND(SUM(total_turnover), 0)   AS "total_turnover",
                ROUND(((SUM(total_turnover)/SUM(year_total_turnover))*100), 1) AS "turnover_percentage"	
           FROM (
                  SELECT year_trade1 AS "year_trade",
                         region,
                         total_turnover,
                         year_total_turnover
                    FROM (
                           SELECT YEAR(date_trade) AS "year_trade1",
			                      region,
	                              ROUND(SUM(turnover), 0) AS "total_turnover"
                             FROM trade
                            GROUP BY region, YEAR(date_trade) ) AS s1
                                  LEFT JOIN (
                                              SELECT YEAR(date_trade) AS "year_trade",
	                                                 ROUND(SUM(turnover), 0) AS "year_total_turnover"
                                                FROM trade
				                               GROUP BY YEAR(date_trade) ) AS s2
	                              ON s1.year_trade1=s2.year_trade) AS s3
GROUP BY region, year_trade
 ORDER BY turnover_percentage Desc) AS s4
 ORDER BY year_trade Desc,
          turnover_percentage Desc;

/* создание представления 'trade_category'/
creation of view 'trade_category'*/
CREATE VIEW trade_category
AS SELECT *
     FROM (
         WITH t3 AS (
              SELECT YEAR(date_trade) AS "year_trade",
                     category,
	                 ROUND(SUM(turnover), 0) AS "total_turnover",
                     ROUND(AVG(avg_check), 0) AS "avg_check"
                FROM trade
               GROUP BY category, YEAR(date_trade)
)
SELECT category,
       year_trade,
       total_turnover,	
       ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage",
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
  FROM t3
 ORDER BY year_trade Desc) AS t4;

/* выборка и сравнение оборота (total_turnover) и среднего чека (avg_check) по категориям в 2022 и 2021 годах (yoy) из представления 'trade_category'/
selection and сomparison of turnover and average check by categories in 2022 and 2021 years (yoy) from view 'trade_category'*/
SELECT category,
       year_trade,
       total_turnover,	
       ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage",
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
  FROM trade_category
 ORDER BY year_trade Desc,
          total_turnover Desc;
          
/* выборка оборота (total_turnover), выборка доли оборота (turnover_percentage) и
сравнение доли оборота (yoy_turnover_percentage) по категориям в 2022 и 2021 годах /
selection of turnover (total_turnover), selection of turnover share (turnover_percentage) and
comparison of turnover share (yoy_turnover_percentage) by categories in 2022 and 2021 years */
SELECT year_trade,
	   category,
	   total_turnover,
       turnover_percentage,
       ROUND((turnover_percentage - LAG(turnover_percentage, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)), 2) AS "yoy_turnover_percentage"
  FROM (
         SELECT year_trade,
	            category,
	            ROUND(SUM(total_turnover), 0)   AS "total_turnover",
                ROUND(((SUM(total_turnover)/SUM(year_total_turnover))*100), 1) AS "turnover_percentage"	
           FROM (
                  SELECT year_trade1 AS "year_trade",
                         category,
                         total_turnover,
                         year_total_turnover
                    FROM (
                           SELECT YEAR(date_trade) AS "year_trade1",
			                      category,
	                              ROUND(SUM(turnover), 0) AS "total_turnover"
                             FROM trade
                            GROUP BY category, YEAR(date_trade) ) AS s1
                                  LEFT JOIN (
                                              SELECT YEAR(date_trade) AS "year_trade",
	                                                 ROUND(SUM(turnover), 0) AS "year_total_turnover"
                                                FROM trade
				                               GROUP BY YEAR(date_trade) ) AS s2
	                              ON s1.year_trade1=s2.year_trade) AS s3
GROUP BY category, year_trade
 ORDER BY turnover_percentage Desc) AS s4
 ORDER BY year_trade Desc,
          turnover_percentage Desc;

/*выборка ТОП-5 категорий по обороту в 2022 г./
selection of TOP-5 categories by turnover in 2022*/
WITH t4 AS (
    SELECT YEAR(date_trade) AS "year_trade",
           category, 
	       ROUND(SUM(turnover), 0) AS "total_turnover"
      FROM trade
     WHERE YEAR(date_trade) = "2022"
     GROUP BY YEAR(date_trade), category
     ORDER BY total_turnover Desc
     LIMIT 5
)
SELECT *
  FROM t4;

 /* выборка оборота ТОП-5 категорий (top5_total_turnover), общего оборота (year_total_turnover),
 доли ТОП-5 категорий в общем обороте в 2022 г. (top5_percentage)/
selection of turnover of TOP-5 categories (top5_total_turnover),
total turnover (year_total_turnover), share of TOP-5 categories by turnover in 2022 (top5_percentage) */
SELECT year_trade1 AS "year_trade",
	   top5_total_turnover,
	   year_total_turnover,
	   ROUND((top5_total_turnover/year_total_turnover)*100, 0) AS "top5_percentage"
  FROM (
        SELECT YEAR(date_trade) AS "year_trade1",
			   ROUND(SUM(turnover), 0) AS "top5_total_turnover"
		  FROM trade
		 WHERE YEAR(date_trade) = "2022" AND category IN ('Digital & Home Appliances','Furniture and household goods','Clothing and shoes','Food','Beauty & Health')
		 GROUP BY YEAR(date_trade)) AS s1
               LEFT JOIN (
                           SELECT YEAR(date_trade) AS "year_trade",
								  ROUND(SUM(turnover), 0) AS "year_total_turnover"
                             FROM trade
				            GROUP BY YEAR(date_trade)) AS s2
				ON s1.year_trade1=s2.year_trade;  

/* выборка оборота ТОП-5 категорий (top5_total_turnover), общего оборота (year_total_turnover), доли ТОП-5 категорий в общем обороте (top5_percentage) по кварталам в 2022 г./
selection of turnover of TOP-5 categories (top5_total_turnover), total turnover (year_total_turnover), share of TOP-5 categories by turnover (top5_percentage) by quarters in 2022 */
SELECT year_trade1 AS "year_trade",
       quarter_trade1 AS "quarter_trade",
	   top5_total_turnover,
	   year_total_turnover,
	   ROUND((top5_total_turnover/year_total_turnover)*100, 0) AS "top5_percentage"
  FROM (
		SELECT YEAR(date_trade) AS "year_trade1",
			   QUARTER(date_trade) AS "quarter_trade1",
			   ROUND(SUM(turnover), 0) AS "top5_total_turnover"
		  FROM trade
		 WHERE YEAR(date_trade) = "2022" AND category IN ('Digital & Home Appliances','Furniture and household goods','Clothing and shoes','Food','Beauty & Health')
		 GROUP BY YEAR(date_trade), QUARTER(date_trade)) AS s1
			   LEFT JOIN (
                          SELECT YEAR(date_trade) AS "year_trade",
                                 QUARTER(date_trade) AS "quarter_trade",
								 ROUND(SUM(turnover), 0) AS "year_total_turnover"
						    FROM trade
						   GROUP BY YEAR(date_trade), QUARTER(date_trade)) AS s2
			   ON s1.year_trade1=s2.year_trade AND s1.quarter_trade1=s2.quarter_trade;

/* создание таблицы `population_base`: регион, население/
creation of table `population_base`: region, population*/
CREATE TABLE population_base (
	region VARCHAR(40) PRIMARY KEY,
	population INT
);

/* создание таблицы-справочника регионов `data_book`: регион из таблицы "trade", регион из таблицы `population_base`
/ creation of table-data book of regions `data_book`: region from table "trade", region from table `population_base`*/
CREATE TABLE data_book (
	region_trade VARCHAR(40) PRIMARY KEY,
	region_data_book VARCHAR(40)
);

/* добавление внешнего ключа в таблицу 'trade' /
addition of foreign key into table 'trade' */
ALTER TABLE trade
ADD FOREIGN KEY (region) REFERENCES data_book (region_trade);

/* добавление внешнего ключа в таблицу 'data_book' /
addition of foreign key into table 'data_book' */
ALTER TABLE data_book
ADD FOREIGN KEY (region_data_book) REFERENCES population_base (region);

/* ввод значений из файлов csv в таблицы `population_base`, `data_book` с помощью мастера импорта табличных данных/
import values from files csv into tables `population_base`, `data_book` through Table Data Import Wizard */

/* выборка первых 10-ти строк таблицы `population_base`/
selection of the first 10 rows of the `population_base'*/
SELECT *
  FROM population_base
 LIMIT 10;
 
/* выборка первых 10-ти строк таблицы `data_book`/
selection of the first 10 rows of the `data_book`*/
SELECT *
  FROM data_book
 LIMIT 10;

/*создание колонки 'population_group' (группа по населению) в таблице 'tab_population'/
creation of column 'population_group' in the table 'tab_population'*/
ALTER TABLE population_base
ADD COLUMN population_group VARCHAR(40);

/*определение группы по населению ([1],[2],[3],[4]) в зависимости от численности населения/
definition of population group ([1],[2],[3],[4]) in order to population base*/
UPDATE population_base
SET population_group = IF(population <= 100000, "[1]<100 thous. people",
                          IF(population > 100000 AND population <= 500000, "[2] thous. people",
                          IF(population > 1000000, "[4]>1 million people", "[3] 500 thous.-1 million people"))
                          );

/* выборка первых 10-ти строк таблицы `population_base`/
selection of the first 10 rows of the `population_base`*/
SELECT *
  FROM population_base
 LIMIT 10;
 
/*определение среднего чека (avg_check_group) по группе по населению,
отклонения среднего чека региона от среднего чека по по группе по населению (deviation_avg_check) в 2022 г./
definition of average check (avg_check_group) by population group,
deviation of average check by region from average check by population group (deviation_avg_check) in 2022 */
WITH t5 AS (
     SELECT year_trade,
            region, 
	        avg_check,
            population_group
       FROM (
            SELECT YEAR(date_trade) AS "year_trade",
                   region, 
	               ROUND(AVG(avg_check), 0) AS "avg_check"
              FROM trade
             WHERE YEAR(date_trade) = "2022"
             GROUP BY YEAR(date_trade), region) AS s1
	               LEFT JOIN (
				              SELECT region_trade, population_group
				                FROM data_book
					                 LEFT JOIN (
						                        SELECT region, population_group
										          FROM population_base) AS s2
					                 ON data_book.region_data_book = s2.region) AS s3
	               ON s1.region = s3.region_trade
)
SELECT year_trade,
       region,
       avg_check,
       ROUND(AVG(avg_check) OVER (
             PARTITION BY population_group
             ORDER BY year_trade), 0) AS "avg_check_group",
	   ROUND((avg_check - AVG(avg_check) OVER (
             PARTITION BY population_group
             ORDER BY year_trade)) / AVG(avg_check) OVER (
             PARTITION BY population_group
             ORDER BY year_trade)*100, 0) AS "deviation_avg_check",
	   population_group
  FROM t5
  ORDER BY avg_check Desc;

/*определение среднего чека (avg_check_group) по группе по населению,
отклонения среднего чека региона от среднего чека по по группе по населению (deviation_avg_check) по кварталам в 2022 г./
definition of average check (avg_check_group) by population group,
deviation of average check by region from average check by population group (deviation_avg_check) by quarters in 2022 */
WITH t6 AS (
     SELECT year_trade,
            quarter_trade,
            region, 
	        avg_check,
            population_group
       FROM (
            SELECT YEAR(date_trade) AS "year_trade",
                   QUARTER(date_trade) AS "quarter_trade",
                   region, 
	               ROUND(AVG(avg_check), 0) AS "avg_check"
              FROM trade
             WHERE YEAR(date_trade) = "2022"
             GROUP BY YEAR(date_trade), QUARTER(date_trade), region) AS s1
	               LEFT JOIN (
				              SELECT region_trade, population_group
				                FROM data_book
					                 LEFT JOIN (
						                        SELECT region, population_group
										          FROM population_base) AS s2
					                 ON data_book.region_data_book = s2.region) AS s3
	               ON s1.region = s3.region_trade
)
SELECT year_trade,
       quarter_trade,
       region,
       avg_check,
       ROUND(AVG(avg_check) OVER (
             PARTITION BY population_group, quarter_trade
             ORDER BY year_trade), 0) AS "avg_check_group",
	   ROUND((avg_check - AVG(avg_check) OVER (
             PARTITION BY population_group, quarter_trade
             ORDER BY year_trade)) / AVG(avg_check) OVER (
             PARTITION BY population_group, quarter_trade
             ORDER BY year_trade)*100, 0) AS "deviation_avg_check",
	   population_group
  FROM t6
  ORDER BY region, quarter_trade;
  
/*выборка 3х регионов с наибольшими темпами роста оборота 'yoy_turnover_percentage' в 2022 г. из представления 'trade_region'/
selection of 3 regions with highest rate of growth of turnover 'yoy_turnover_percentage' from view 'trade_region'*/
SELECT region,
       year_trade,
       total_turnover,	
       ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage"        
FROM trade_region
ORDER BY year_trade Desc, yoy_turnover_percentage Desc
LIMIT 3;

/*выборка 3х регионов с наименьшими темпами роста оборота 'yoy_turnover_percentage' в 2022 г. из представления 'trade_region'/
selection of 3 regions with lowest rate of growth of turnover 'yoy_turnover_percentage' from view 'trade_region'*/
SELECT region,
       year_trade,
       total_turnover,	
       ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage"        
FROM trade_region
ORDER BY year_trade Desc, yoy_turnover_percentage Asc
LIMIT 3;

/*выборка 3х регионов с наибольшими темпами роста среднего чека 'yoy_avg_check_percentage' в 2022 г. из представления 'trade_category'/
selection of 3 regions with highest rate of growth of average check 'yoy_avg_check_percentage' from view 'trade_category'*/
SELECT region,
       year_trade,
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
FROM trade_region
ORDER BY year_trade Desc, yoy_avg_check_percentage Desc
LIMIT 3;

/*выборка 3х регионов с наименьшими темпами роста среднего чека 'yoy_avg_check_percentage' в 2022 г. из представления 'trade_region'/
selection of 3 regions with lowest rate of growth of average check 'yoy_avg_check_percentage' from view 'trade_region'*/
SELECT region,
       year_trade,
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY region
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
FROM trade_region
ORDER BY year_trade Desc, yoy_avg_check_percentage Asc
LIMIT 3;

/*выборка 3х категорий товаров с наибольшими темпами роста оборота 'yoy_turnover_percentage' в 2022 г. из представления 'trade_category'/
selection of 3 categories of goods with highest rate of growth of turnover 'yoy_turnover_percentage' from view 'trade_region'*/
SELECT category,
	   year_trade,
	   total_turnover,	
	   ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage"        
FROM trade_category
ORDER BY year_trade Desc, yoy_turnover_percentage Desc
LIMIT 3;

/*выборка 3х категорий товаров с наименьшими темпами роста оборота 'yoy_turnover_percentage' в 2022 г. из представления 'trade_region'/
selection of 3 categories of goods with lowest rate of growth of turnover 'yoy_turnover_percentage' from view 'trade_region'*/
SELECT category,
	   year_trade,
	   total_turnover,	
	   ROUND((total_turnover - LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(total_turnover, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_turnover_percentage"        
FROM trade_category
ORDER BY year_trade Desc, yoy_turnover_percentage Asc
LIMIT 3;

/*выборка 3х категорий товаров с наибольшими темпами роста среднего чека 'yoy_avg_check_percentage' в 2022 г. из представления 'trade_category'/
selection of 3 categories of goods with highest rate of growth of average check 'yoy_avg_check_percentage' from view 'trade_category'*/
SELECT category,
       year_trade,
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
FROM trade_category
ORDER BY year_trade Desc, yoy_avg_check_percentage Desc
LIMIT 3;

/*выборка 3х категорий товаров с наименьшими темпами роста среднего чека 'yoy_avg_check_percentage' в 2022 г. из представления 'trade_region'/
selection of 3 categories of goods with lowest rate of growth of average check 'yoy_avg_check_percentage' from view 'trade_region'*/
SELECT category,
       year_trade,
       avg_check,
       ROUND((avg_check - LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)) / LAG(avg_check, 1) OVER (
             PARTITION BY category
             ORDER BY year_trade)*100, 0) AS "yoy_avg_check_percentage"
FROM trade_category
ORDER BY year_trade Desc, yoy_avg_check_percentage Asc
LIMIT 3;