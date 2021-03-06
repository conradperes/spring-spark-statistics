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

import static java.util.stream.Collectors.toList;
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
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(toList());
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
        }).collect(toList());
    }


    public int count404() {
        Dataset errors = getDataset(HADOOP_URI , FILE_NAME);
        // Counts all the errors
        errors.count();
        // Counts errors mentioning MySQL
        errors.filter(col("line").like("%MySQL%")).count();
        // Fetches the MySQL errors as an array of strings
        errors.filter(col("line").like("%MySQL%")).collect();
        return (int) errors.count();
    }

    private Dataset getDataset(String hadoopUri, String fileName) {
        int partitionCount = 100;
        SparkContext sc = sparkSession.sparkContext();
        RDD<String> lines = sc.textFile(HADOOP_URI + FILE_NAME, 3 * sc.defaultParallelism()).cache();
        long count = lines.count();
        JavaRDD<Row> rowRDD = lines.toJavaRDD().map(RowFactory::create);
        //List<StructField> fields = Arrays.asList(
        //        DataTypes.createStructField("line", DataTypes.StringType, true));
        StructType schema =
         DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("url",  DataTypes.StringType, true),
                DataTypes.createStructField("data", DataTypes.TimestampType, true),
                DataTypes.createStructField("chamada", DataTypes.StringType, true),
                DataTypes.createStructField("httpcode", DataTypes.IntegerType, true),
                DataTypes.createStructField("qtde", DataTypes.IntegerType, true)
        });
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);
        return df.filter(col("httpcode").endsWith("404 -"));
    }

//    public List<Count> count404GroupedByDay() {
//        int partitionCount = 100;
//        SparkContext sc = sparkSession.sparkContext();
//        RDD<String> lines = sc.textFile(HADOOP_URI + FILE_NAME, 3 * sc.defaultParallelism()).cache();
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//
//            private static final long serialVersionUID = 1L;
//
//            // Here the first line will be passed, for example, hello you.
//            // The Iterable<String>(hello, you) is returned.
//            @Override
//            public Iterator<String> call(String t) throws Exception {
//                return Arrays.asList(t.endsWith("404 -");
//            }
//
//        });
//
//        long count = lines.count();
//
//        //List<Word>words  = (List<Word>) lines.toJavaRDD().map(Word::new);
//        //List<StructField> fields = Arrays.asList(
//        //        DataTypes.createStructField("line",  DataTypes.StringType, true));
//        //StructType schema = DataTypes.createStructType(fields);
//        SQLContext sqlContext = new SQLContext(sc);
//        StructType schema = DataTypes.createStructType(new StructField[] {
//                DataTypes.createStructField("url",  DataTypes.StringType, true),
//                DataTypes.createStructField("data", DataTypes.TimestampType, true),
//                DataTypes.createStructField("chamada", DataTypes.StringType, true),
//                DataTypes.createStructField("httpcode", DataTypes.IntegerType, true),
//                DataTypes.createStructField("qtde", DataTypes.IntegerType, true)
//        });
//        Dataset<Row> df = sqlContext.createDataFrame(lines, schema);
//        Dataset errors = df.filter(col("httpcode").endsWith("404 -"));
//        // Counts all the errors
//        errors.count();
//        // Counts errors mentioning MySQL
//        errors.filter(col("httpcode").like("%MySQL%")).count();
//        // Fetches the MySQL errors as an array of strings
//        errors.filter(col("httpcode").like("%MySQL%")).collect();
//
//        RelationalGroupedDataset groupedDataset = errors.groupBy(col("data"));
//        System.out.println("####################" +
//                "Total de 404 agrupados por data:"+groupedDataset.count()
//        +"####################");
//        groupedDataset.count().show();
//        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
//        return rows.stream().map(new Function<Row, Count>() {
//            @Override
//            public Count apply(Row row) {
//                return new Count(row.getString(0), row.getLong(1));
//            }
//        }).collect(toList());
//
//    }




    public List<Count> groupBy() {
        //        String input = "hello world hello hello hello";
        Dataset errors = getDataset(HADOOP_URI , FILE_NAME);
        //List<Word>words = errors.collectAsList().stream().map(Word::new).collect(toList());

        RelationalGroupedDataset groupedDataset = errors.groupBy(col("data"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count();
            }
        }).collect(toList());
    }


}