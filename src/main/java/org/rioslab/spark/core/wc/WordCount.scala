package org.rioslab.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

    def main(args: Array[String]): Unit = {
        // 连接到Spark框架

        val config = new SparkConf()
            .setMaster("local[*]")
            .setAppName("WordCount Application")

        val sc = new SparkContext(config)

        // 统计词频
        val RDD: RDD[String] = sc.textFile("patent/patent_cleaned.csv")

        val lines: RDD[Array[String]] = RDD.map { line => line.split(",") }


//        lines.foreach(f => println(f.mkString(",")))
        val textRDD = lines.map(
            f = cols => {
                val text = cols(3) + cols(4)
                println(cols.length)
                text
            }
        )

        val splitText = textRDD.flatMap(_.split(" "))
        val wordGroup = splitText.groupBy(word => word)


        val wordCount = wordGroup.map {
            case (str, value) => {
                (str, value.size)
            }
        }

        val countArr = wordCount.collect()

        countArr.foreach(println)



        // 关闭连接
        sc.stop()
    }

}
