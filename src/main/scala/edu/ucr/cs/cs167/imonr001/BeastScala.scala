package edu.ucr.cs.cs167.imonr001

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession


/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "grouped-aggregate" =>

            sparkSession.read.parquet(inputFile)
              .createOrReplaceTempView("parquetFile")
            println("OBSERVATION_COUNT")
            sparkSession.sql(
              s"""
              SELECT ZIPCODE,sum(OBSERVATION_COUNT) as observation_per_zipcode
              FROM parquetFile
              group by ZIPCODE
              """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))

        case "second-grouped-aggregate" =>
          val keyword: String = args(2)
          val df = sparkSession.read.parquet(inputFile)
          print(df.schema)
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("parquetFile")
          println("OBSERVATION_COUNT")
          sparkSession.sql(
            s"""
              SELECT ZIPCODE,sum(OBSERVATION_COUNT) as observation_per_zipcode
              FROM parquetFile
              WHERE COMMON_NAME == "$keyword"
              GROUP BY ZIPCODE
              ORDER BY observation_per_zipcode
              """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))
        case "ratio-aggregate" =>
          val keyword: String = args(2)
          val df = sparkSession.read.parquet(inputFile)
          print(df.schema)
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("parquetFile")
          println("OBSERVATION_COUNT")
          sparkSession.sql(
            s"""
              SELECT t1.ZIPCode, avg(t1.observation_per_zipcode / t2.total) as ratio
              from
              (SELECT ZIPCode, sum(OBSERVATION_COUNT) as observation_per_zipcode
                FROM parquetFile
                WHERE COMMON_NAME == "$keyword"
                GROUP BY ZIPCode
                ORDER BY observation_per_zipcode) as t1
              join
              (SELECT ZIPCode, sum(OBSERVATION_COUNT) as total
              FROM parquetFile
              GROUP BY ZIPCode
              ORDER BY total) as t2
              on t1.ZIPCode = t2.ZIPCode
              GROUP BY t1.ZIPCode, t2.ZIPCode
              ORDER BY ratio ASC
              """).show()

        case "choropleth-map" =>
          val keyword: String = args(2)
          val eBirdZIPCodeRatio: String = args(3)
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("parquetFile")
            println("observation_per_zipcode")

          sparkSession.sql(
            s"""
            select t1.ZIPCode, avg(t1.observation_per_zipcode / t2.total) as ratio from
              (SELECT ZIPCode, sum(OBSERVATION_COUNT) as observation_per_zipcode
                FROM parquetFile
                WHERE COMMON_NAME == "$keyword"
                GROUP BY ZIPCode
                ORDER BY observation_per_zipcode) as t1
            join
            (SELECT ZIPCode, sum(OBSERVATION_COUNT) as total
              FROM parquetFile
              GROUP BY ZIPCode
              ORDER BY total) as t2
            on t1.ZIPCode = t2.ZIPCode
            GROUP BY t1.ZIPCode, t2.ZIPCode
            ORDER BY ratio ASC
            """).createOrReplaceTempView("ZIPCode")


          sparkContext.shapefile("tl_2018_us_zcta510.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("ZCTA5CE10")

          sparkSession.sql(
            s"""
                  SELECT ZIPCode, g, ratio
                  FROM ZIPCode, ZCTA5CE10
                  WHERE ZIPCode = GEOID10
                  """)
                  .show()

          sparkSession.sql(
            s"""
                  SELECT ZIPCode, g, ratio
                  FROM ZIPCode, ZCTA5CE10
                  WHERE ZIPCode = GEOID10
                  """)
                  .toSpatialRDD
                  .coalesce(1)
                  .saveAsShapefile(eBirdZIPCodeRatio)

        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}
