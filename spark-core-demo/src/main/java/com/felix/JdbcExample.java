package com.felix;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * https://github.com/KinoMin/bigdata-learning-notes/blob/master/note/spark/Spark%E8%AF%BB%E5%8F%96JDBC%E6%95%B0%E6%8D%AE%E6%BA%90%E4%BC%98%E5%8C%96.md
 */
public class JdbcExample {

    public static void main(String[] args) {
//        jdbcRead();
        jdbcWrite();
    }

    private static void jdbcRead() {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "123456");
        prop.put("driver", "com.mysql.cj.jdbc.Driver");
        String table = "t_order";
//        String table = "(select * from t_order where order_id <= 100) as t";
        Dataset<Row> df = spark.read()
                .jdbc("jdbc:mysql://192.168.159.111:3306/oper_db?useUnicode=true&characterEncoding=utf-8&useSSL=false"
                        , table, prop);
        System.out.println("==========> 分区数: " + df.javaRDD().partitions().size());
        long count = df.count();
        System.out.println("==========> 条数: " + count);
        df.show();
        spark.stop();
    }

    private static void jdbcWrite() {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "123456");
        prop.put("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> source = spark.read().json("F:/Idea Projects/spark-cookbook/spark-core-demo/data/input/t_order.json");
        source.createOrReplaceTempView("o");
        Dataset<Row> orders = spark.sql("SELECT order_no, customer_id, order_time, order_status, order_amount, goods_id, created_time, updated_time " +
                "FROM o where order_id = 1");

        orders.write()
              .mode(SaveMode.Append)
              .jdbc("jdbc:mysql://192.168.159.111:3306/oper_db?useUnicode=true&characterEncoding=utf-8&useSSL=false", "t_order", prop);

        spark.stop();
    }
}
