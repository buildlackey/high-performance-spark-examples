package com.highperformancespark.examples.dataframe

import com.highperformancespark.examples.dataframe.RawPanda
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext}



object PandaExerciser extends App {
  
  val sc = new SparkContext("local[2]", "Panda-o-rama")
  val sqlContext=new SQLContext(sc);

  val session=sqlContext.sparkSession

  val p1 = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
  val p2 = RawPanda(1, "M1B 6K7", "giant", false, Array(0.1, 0.1))

  val pandaInfo= session.createDataFrame(Seq(p1, p2))
  val result: Dataset[Row] = pandaInfo.filter(pandaInfo("happy") !== true)
  println("result:" + result);
  result.show()
}
