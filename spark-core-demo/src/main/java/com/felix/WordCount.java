package com.felix;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * https://github.com/nivdul/spark-in-practice/
 */
public class WordCount {

    private static final String textFilePath = "spark-core-demo/data/input/words.txt";

    public static void main(String[] args) throws Exception {
        wordCountByDataFrame();
    }

    /**
     * spark sql word count
     */
    private static void wordCountBySql() {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession sc = SparkSession.builder()
                .appName("WordCount")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> lines = sc.read().textFile(textFilePath);

        Dataset<Row> rows = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).iterator();
            }
        }, Encoders.STRING()).toDF("name");

        rows.createOrReplaceTempView("t_words");
        Dataset<Row> counts = sc.sql("SELECT name, COUNT(1) as sum FROM t_words GROUP BY name ORDER BY sum DESC");
        counts.show();

        sc.stop();
    }

    /**
     * dataFrame word count
     */
    private static void wordCountByDataFrame() {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession sc = SparkSession.builder()
                .appName("WordCount")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> lines = sc.read().textFile(textFilePath);
        Dataset<Row> rows = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                return Arrays.stream(s.split(" ")).map(x -> Tuple2.apply(x, 1)).iterator();
            }
        }, Encoders.tuple(Encoders.STRING(), Encoders.INT())).toDF("name", "cnt");
        Dataset<Row> counts = rows.select(functions.col("name"), functions.col("cnt"))
                .groupBy(functions.col("name"))
                .agg(functions.count("cnt").as("sum"))
                .orderBy(functions.desc("sum"));
        counts.show();

        sc.stop();
    }

    /**
     * rdd word count
     */
    private static void wordCountByRDD() {
        //解决hadoop依赖
        System.setProperty("hadoop.home.dir", "F:/Idea Projects/spark-cookbook/spark-core-demo/data/ext");
        SparkSession sparkSession = SparkSession.builder()
                .appName("WordCount")
                .master("local[*]")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jSc = JavaSparkContext.fromSparkContext(sc);
        JavaRDD<String> words = jSc.textFile(textFilePath);
        JavaPairRDD<String, Integer> pairRdd = words
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator())
                .mapToPair(x -> Tuple2.apply(x, 1));
        JavaPairRDD<String, Integer> cntRdd = pairRdd.reduceByKey((a, b) -> a + b);
        JavaPairRDD<Integer, String> cntKeyRdd = cntRdd.mapToPair(x -> new Tuple2<>(x._2, x._1));
        JavaPairRDD<Integer, String> descCntRdd = cntKeyRdd.sortByKey(false);
        StructType structType = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("cnt", DataTypes.IntegerType, true)
                , DataTypes.createStructField("name", DataTypes.StringType, true)
        });
        JavaRDD<Row> rowRdd = descCntRdd.map(x -> RowFactory.create(x._1, x._2));
        Dataset<Row> df = sparkSession.createDataFrame(rowRdd, structType);
        df.repartition(1).write()
                .format("com.databricks.spark.csv")
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ",")
                .csv("spark-core-demo/data/output/1");
        sc.stop();
    }
}
