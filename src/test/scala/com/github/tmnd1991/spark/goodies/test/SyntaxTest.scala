package com.github.tmnd1991.spark.goodies.test
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import java.time.Duration

class SyntaxTest {
  val spark: SparkSession = null
  import spark.implicits._
  val df: Dataset[Int] = spark.createDataset(List(1, 2, 3, 4))
  def noTimeoutTest() = {
    import com.github.tmnd1991.spark.goodies.noTimeout._
    df.groupByKey(_.toString.head.toString).outputMode(OutputMode.Update()).flatmapGroupsWithState[Boolean, String] {
      case (key, values, s @ NoneState(currentTs)) =>
        println(currentTs)
        s -> List(values.mkString("-") + key)
      case (key, values, s @ SomeState(currentTs, stateContent)) =>
        println(stateContent)
        println(currentTs)
        s -> List(values.mkString("-") + key)
    }
  }

  def eventTimeoutTest() = {
    import com.github.tmnd1991.spark.goodies.eventTimeout._
    df.groupByKey(_.toString.head.toString).outputMode(OutputMode.Update()).flatmapGroupsWithState[Boolean, String] {
      case (key, values, s @ NoneState(currentTs)) =>
        println(currentTs)
        s -> List(values.mkString("-") + key)
      case (key, values, s @ TimedOutState(_,currentTs, stateContent)) =>
        println(stateContent)
        s.timeout(currentTs + 20000) -> List(values.mkString("-") + key)
      case (key, _, s @ SomeState(_,currentTs, stateContent)) =>
        println(stateContent)
        s.timeout(currentTs + 20000) -> List(key)
    }
  }

  def processingTimeoutTest() = {
    import com.github.tmnd1991.spark.goodies.processingTimeout._
    df.groupByKey(_.toString.head.toString).outputMode(OutputMode.Update()).flatmapGroupsWithState[Boolean, String] {
      case (key, values, s @ NoneState(currentTs)) =>
        println(currentTs)
        s -> List(values.mkString("-") + key)
      case (key, values, s @ TimedOutState(_,currentTs, stateContent)) =>
        println(stateContent)
        println(currentTs)
        s.timeout(Duration.ofMillis(20000)) -> List(values.mkString("-") + key)
      case (key, _, s @ SomeState(_,currentTs, stateContent)) =>
        println(stateContent)
        println(currentTs)
        s.timeout(Duration.ofMillis(20000)) -> List(key)
    }
  }
}
