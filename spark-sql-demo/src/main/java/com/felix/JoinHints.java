package com.felix;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

/**
 * https://spark.apache.org/docs/3.2.0/sql-ref-syntax-qry-select-hints.html
 */
public class JoinHints {

    private static final String jsonFilePath = "spark-sql-demo/data/input/reduced-tweets.json";
    private static final String dimCountryFilePath = "spark-sql-demo/data/input/dim-country.json";

    public static void main(String[] args) throws Exception {
        broadcastJoinSql();
    }

    private static void broadcastJoinSql() throws Exception {
        SparkSession sc = loadData();
        Dataset<Row> country = sc.sql("SELECT /*+ BROADCAST(c) */ * FROM t left join c on t.country = c.country");
        country.explain();
    }

    @NotNull
    private static SparkSession loadData() throws AnalysisException {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-sql-demo/data/ext");
        SparkSession sc = SparkSession.builder()
                .appName("JoinHints")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> source = sc.read().json(jsonFilePath);
        source.createTempView("t");
        Dataset<Row> country = sc.read().json(dimCountryFilePath);
        country.createTempView("c");
        return sc;
    }
}
