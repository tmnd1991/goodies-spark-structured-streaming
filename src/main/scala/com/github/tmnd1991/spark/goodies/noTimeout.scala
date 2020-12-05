package com.github.tmnd1991.spark.goodies

import com.github.tmnd1991.spark.goodies.impl.DatasetWithStateNoTimeout
import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.streaming.OutputMode

object noTimeout {
  sealed trait State[+S] {
    val currentTs: Long
    def set[A](f: => A): State[A]
    def map[A](f: S => A): State[A]
    def flatMap[A](f: S => Option[A]): State[A]
    def fold[A](ifEmpty: => A)(f: S => A): State[A]
    def empty[A]: State[A]
  }

  case class NoneState[+S](currentTs: Long) extends State[S] {

    override def set[A](f: => A): State[A] = SomeState(currentTs, f)

    override def map[A](f: S => A): State[A] = this.copy[A]()

    override def flatMap[A](f: S => Option[A]): State[A] = this.copy[A]()

    override def fold[A](ifEmpty: => A)(f: S => A): State[A] = SomeState[A](currentTs, ifEmpty)

    override def empty[A]: State[A] = this.copy[A]()
  }

  case class SomeState[+S](currentTs: Long, s: S) extends State[S] {
    override def set[A](a: => A): State[A] = SomeState(currentTs, a)

    override def map[A](f: S => A): State[A] = this.copy(s = f(s))

    override def flatMap[A](f: S => Option[A]): State[A] =
      f(s).fold[State[A]](NoneState[A](currentTs))(r => this.copy(s = r))

    override def fold[A](ifEmpty: => A)(f: S => A): State[A] =
      this.copy(s = f(s))

    override def empty[A]: State[A] = NoneState[A](currentTs)
  }
  def build[K, V](g: KeyValueGroupedDataset[K, V], o: OutputMode): DatasetWithStateNoTimeout[K, V] =
    new DatasetWithStateNoTimeout(g, o)
  implicit class RDatasetWithStateNoTimeout[K, V](df: KeyValueGroupedDataset[K, V]) {
    def outputMode(o: OutputMode): DatasetWithStateNoTimeout[K, V] =
      new DatasetWithStateNoTimeout(df, o)
  }
}
