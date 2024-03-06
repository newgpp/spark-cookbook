package com.felix;

import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;

/**
 * https://spark.apache.org/docs/3.2.0/sql-ref-syntax-qry-select-hints.html
 */
public class PartitioningHints {

    private static final String jsonFilePath = "spark-core-demo/data/input/reduced-tweets.json";
    private static final String dimCountryFilePath = "spark-core-demo/data/input/dim-country.json";

    public static void main(String[] args) throws Exception {
        repartitionSql();
    }

    private static void repartitionSql() throws Exception {
        SparkSession sc = loadData();
        Dataset<Row> country = sc.sql("SELECT /*+ REPARTITION(8, country) */ * FROM t");
        country.explain();
    }

    @NotNull
    private static SparkSession loadData() throws AnalysisException {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession sc = SparkSession.builder()
                .appName("PartitioningHints")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> source = sc.read().json(jsonFilePath).withColumn("id", functions.monotonically_increasing_id());
        source.createTempView("t");
        return sc;
    }
}
