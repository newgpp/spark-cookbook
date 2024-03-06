package com.felix;

import org.apache.spark.sql.*;

/**
 * https://spark.apache.org/docs/3.2.0/sql-ref-syntax-qry-select-window.html
 */
public class WindowSqlExample {
    private static final String jsonFilePath = "spark-core-demo/data/input/reduced-tweets.json";

    public static void main(String[] args) throws Exception {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession sc = SparkSession.builder()
                .appName("OverSqlExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> source = sc.read().json(jsonFilePath).withColumn("id", functions.monotonically_increasing_id());


        source.createTempView("t");


        Dataset<Row> country = sc.sql("with a as (select country, id, rank() over (partition by country order by id asc) as rank from t) select country, id from a where rank = 1");

        country.write().mode(SaveMode.Overwrite).json("spark-sql-demo/data/input/dim_country");

        sc.stop();
    }
}
