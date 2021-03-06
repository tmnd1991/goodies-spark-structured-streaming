package com.github.tmnd1991.spark.goodies

import com.github.tmnd1991.spark.goodies.impl.DatasetWithStateEventTimeout
import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.streaming.OutputMode

object eventTimeout {
  sealed trait State[+S] {
    val currentTs: Long
    def set[A](f: => A): State[A]
    def map[A](f: S => A): State[A]
    def flatMap[A](f: S => Option[A]): State[A]
    def fold[A](ifEmpty: => A)(f: S => A): State[A]
    def empty[A]: State[A]
  }

  case class NoneState[+S](currentTs: Long) extends State[S] {

    override def set[A](f: => A): SomeState[A] = SomeState(None, currentTs, f)

    override def map[A](f: S => A): State[A] = this.copy[A]()

    override def flatMap[A](f: S => Option[A]): State[A] = this.copy[A]()

    override def fold[A](ifEmpty: => A)(f: S => A): SomeState[A] = SomeState[A](None, currentTs, ifEmpty)

    override def empty[A]: NoneState[A] = this.copy[A]()
  }

  case class TimedOutState[+S](timeout: Option[Long], currentTs: Long, s: S) extends State[S] {
    override def set[A](a: => A): TimedOutState[A] = TimedOutState(timeout, currentTs, a)

    override def map[A](f: S => A): TimedOutState[A] = this.copy(s = f(s))

    override def flatMap[A](f: S => Option[A]): State[A] =
      f(s).fold[State[A]](NoneState[A](currentTs))(r => this.copy(s = r))

    override def fold[A](ifEmpty: => A)(f: S => A): TimedOutState[A] =
      this.copy(s = f(s))

    override def empty[A]: State[A] = NoneState[A](currentTs)

    def timeout(timeout: Long): TimedOutState[S] = this.copy(timeout = Some(timeout))
  }

  case class SomeState[+S](timeout: Option[Long], currentTs: Long, s: S) extends State[S] {
    override def set[A](a: => A): SomeState[A] = SomeState(timeout, currentTs, a)

    override def map[A](f: S => A): SomeState[A] = this.copy(s = f(s))

    override def flatMap[A](f: S => Option[A]): State[A] =
      f(s).fold[State[A]](NoneState[A](currentTs))(r => this.copy(s = r))

    override def fold[A](ifEmpty: => A)(f: S => A): SomeState[A] =
      this.copy(s = f(s))

    override def empty[A]: State[A] = NoneState[A](currentTs)

    def timeout(timeout: Long): SomeState[S] = this.copy(timeout = Some(timeout))
  }

  implicit class RDatasetWithStateNoTimeout[K, V](df: KeyValueGroupedDataset[K, V]) {
    def outputMode(o: OutputMode): DatasetWithStateEventTimeout[K, V] =
      new DatasetWithStateEventTimeout(df, o)
  }
}
