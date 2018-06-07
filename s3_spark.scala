import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object readCsv {
  def main(args:Array[String])={
  val x = SparkSession.builder().appName("soumith").master("local").getOrCreate()
  val y = SparkContext.getOrCreate()
  val accessKeyId = "AKIAJAXZGM7APYKXO44Q"
  
  
 val accessSecret = "Aa8fkKO3TINgl38PFOx4mS3KnYtUr8AAdL3u+TpL"
  y.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",accessKeyId)
  y.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",accessSecret)
  val d = y.textFile("s3n://sparkscalap/movies.csv")
  d.take(10).foreach(println)

  }
}
