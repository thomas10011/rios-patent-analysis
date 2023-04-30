package org.rioslab.spark.core.wc

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}

object FeatureGenerator {

    def count(text: String, pattern: String): Int = {
        val n = pattern.length
        val m = text.length
        val next: Array[Int] = new Array[Int](n)

        var i = 1; var j = -1; var cnt = 0
        next(0) = -1
        while (i < n) {
            if (j < 0 || pattern.charAt(j) == text.charAt(i-1)) {
                j += 1
                next(i) = j
                i += 1
            }
            else {
                j = next(j)
            }
        }

        i = 0; j = 0
        while (i < m) {
            if (j == n-1 && pattern.charAt(j) == text.charAt(i)) {
                cnt += 1;
                j = next(j)
            }
            else if (j < 0 || pattern.charAt(j) == text.charAt(i)) {
                i += 1
                j += 1
            }
            else j = next(j)
        }

        cnt
    }

    def join(delimiter: String, texts: Array[String]): String = {
        if (texts.length == 0) return ""
        if (texts.length == 1) return texts(0)

        var ret = texts(0)
        var i = 1
        while (i < texts.length) {
            ret += delimiter + texts(i)
            i += 1
        }
        ret
    }

    def main(args: Array[String]): Unit = {
        val config = new SparkConf()
            .setMaster("local[*]")
            .setAppName("Feature Generate Application")

        val spark = SparkSession.builder().config(config).getOrCreate()
        import spark.implicits._

        val df = spark
            .read
            .option("header", "true")
            .option("multiline", "true")
            .option("escape", "\"")
            .csv("patent/patent_cleaned.csv")

        val phraseDF = spark
            .read
            .option("header", "false")
            .option("multiline", "true")
            .option("escape", "\"")
            .csv("patent/key_phrase.csv")

        val phraseList = phraseDF
            .map(
                line => {
                    var ret = ""
                    if (line(1) != null && line(1).toString.nonEmpty) ret += line(1).toString
                    if (line(2) != null && line(2).toString.nonEmpty) ret += "," + line(2).toString
                    if (line(3) != null && line(3).toString.nonEmpty) ret += "," + line(3).toString
                    ret
                }
            )
            .flatMap(_.split(","))
            .collect()
            .map(
                text => {
                    val ret = text.trim
                    ret.replaceAll("[\n\r\t]", "")
                }
            )
            .filter(_.nonEmpty)
            .toSet // 去重
            .toList

        phraseList.foreach(println)

        println(phraseList.length)

        df.show() // 向控制台打印Dataframe

        val lines = df.map(line => (line(0).toString, line(3).toString + " " + line(4).toString))

        val featList = lines
        .map(
            line => {
                val pub_num: String = line._1
                val text: String = line._2
                val vec = new Array[String](phraseList.length)
                var i = 0
                while (i < phraseList.length) {
                    vec(i) = count(text, phraseList(i)).toString
                    i += 1
                }
                val vecStr = join(",", vec)
                (pub_num, vecStr)
            }
        )
        featList.show()

        featList
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .option("header", "false")
            .csv("output/features.csv")


    }

}
