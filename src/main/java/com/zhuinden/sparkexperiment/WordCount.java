package com.zhuinden.sparkexperiment;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
/**
 * Created by achat1 on 9/23/15.
 * Just an example to see if it works.
 */
@Component
public class WordCount {
    @Autowired
    private SparkSession sparkSession;

    private static final String HADOOP_URI= "hdfs://localhost:9000";
    private static final String FILE_NAME ="/input/access_log_Aug95";
//    static String input;
//    static{
//        String path ="/Users/conradperes/Projects/bigdata/access_log_Aug95";
//        try {
//            input = new String(readAllBytes(Paths.get(path)));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }



    public List<Count> count() {
        String input = "hello world hello hello hello";
        String[] _words = input.split(" ");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());
    }


    public List<Count> count404() {
//        //String input = "hello world hello hello hello";
//        String[] _words = input.split("404");
//        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
//        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
//        dataFrame.show();
//        //StructType structType = dataFrame.schema();
//
//        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
//        groupedDataset.count().show();
//        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
//        return rows.stream().map(new Function<Row, Count>() {
//            @Override
//            public Count apply(Row row) {
//                return new Count(row.getString(0), row.getLong(1));
//            }
//        }).collect(Collectors.toList());
        int partitionCount = 100;
        SparkContext sc = sparkSession.sparkContext();
        RDD<String> lines = sc.textFile(HADOOP_URI + FILE_NAME, 3 * sc.defaultParallelism()).cache();
        long count = lines.count();
        JavaRDD<Row> rowRDD = lines.toJavaRDD().map(RowFactory::create);
//        JavaPairRDD<String, String> skillCompanyRdd = lines.toJavaRDD()
//                .mapToPair(line -> {
//                    String[] parts = line.split("\t");
//                    return new Tuple2<>(parts[0] + "\t" + parts[1], 1);
//                })
//                .reduceByKey((a, b) -> a + b)
//                .map(Tuple2::_1)
//                .mapToPair(line -> {
//                    String[] parts = line.split("\t");
//                    return new Tuple2<>(parts[0],parts[1]);
//                }).coalesce(partitionCount).cache();
//
//        count = skillCompanyRdd.count();

        //lines.unpersist();
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);

        Dataset errors = df.filter(col("line").like("%404%"));
// Counts all the errors
        errors.count();
// Counts errors mentioning MySQL
        errors.filter(col("line").like("%MySQL%")).count();
// Fetches the MySQL errors as an array of strings
        errors.filter(col("line").like("%MySQL%")).collect();
        RelationalGroupedDataset groupedDataset = errors.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());
    }



}