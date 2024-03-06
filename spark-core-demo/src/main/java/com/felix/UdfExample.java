package com.felix;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class UdfExample {

    public static void main(String[] args) {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession sc = SparkSession.builder()
                .appName("WordCount")
                .master("local[*]")
                .getOrCreate();
        List<String> words = Arrays.stream("This section does not cite any references or sources Please help improve this section by adding citations to".split(" "))
                .collect(Collectors.toList());
        Dataset<Row> source = sc.createDataset(words, Encoders.STRING()).toDF("word");
        //生成10条数据
        SQLContext sqlContext = sc.sqlContext();
        sqlContext.udf().register("get_time", new UDF0<String>() {
            @Override
            public String call() throws Exception {
                TimeUnit.MILLISECONDS.sleep(10L);
                return String.valueOf(System.currentTimeMillis());
            }
        }, DataTypes.StringType);
        Dataset<Row> table = source
                .withColumn("a", functions.rand(10L))
                .withColumn("ts", functions.expr("get_time()"));

        table.show();
        sc.stop();
    }

}
