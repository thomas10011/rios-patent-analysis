package org.rioslab.spark.core


object Test {


    def count(text: String, pattern: String): Int = {
        val n = pattern.length
        val m = text.length
        val next: Array[Int] = new Array[Int](n)

        var i = 1;
        var j = -1;
        var cnt = 0
        next(0) = -1
        while (i < n) {
            if (j < 0 || pattern.charAt(j) == text.charAt(i - 1)) {
                j += 1
                next(i) = j
                i += 1
            }
            else {
                j = next(j)
            }
        }

        i = 0;
        j = 0
        while (i < m) {
            if (j == n - 1 && pattern.charAt(j) == text.charAt(i)) {
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

    def main(args: Array[String]): Unit = {
        println("Hello Scala!")
        val string: String = " 你？ \n\r\t     ?hah ,.。，，-$%^@!~&  ^*()  ）（*  &……%  ￥##@！~是+——）=-谁"
        val s: String = string.replaceAll("[\\pP\\pS\\pZ\n\r\t]", "")

        println(count("abcacbn?abc* abc/(sd abcsfdjkh", "abc"))
        println(s)
    }


}
