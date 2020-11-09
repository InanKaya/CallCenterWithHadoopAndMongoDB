import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.1-bin");

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Police CallCenter")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/police.incidentLocation")
                .getOrCreate();

        StructType schema = new StructType()
                .add("recordid", DataTypes.IntegerType)
                .add("calldatetime", DataTypes.StringType)
                .add("priority", DataTypes.StringType)
                .add("district", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("callNumber", DataTypes.StringType)
                .add("incidentLocation", DataTypes.StringType)
                .add("location", DataTypes.StringType);


        Dataset<Row> rawData = sparkSession.read()
                .option("header",true)
                .schema(schema)
                .csv("hdfs://localhost:8020/911-police-calls-for-service-1.csv");

        Dataset<Row> data = rawData.filter(rawData.col("recordid").isNotNull());


        Dataset<Row> descriptionDS = data.filter(data.col("description").notEqual("911/NO  VOICE"));
        Dataset<Row> mongoDS = descriptionDS.groupBy("incidentLocation", "description").count().sort(functions.desc("count"));

        MongoSpark.write(mongoDS).mode("overwrite").save();

    }
}
