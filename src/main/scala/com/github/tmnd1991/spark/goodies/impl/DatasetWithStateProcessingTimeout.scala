package com.github.tmnd1991.spark.goodies.impl

import com.github.tmnd1991.spark.goodies.processingTimeout.{ NoneState, SomeState, State, TimedOutState }
import org.apache.spark.sql.streaming.{ GroupStateTimeout, OutputMode }
import org.apache.spark.sql.{ Dataset, Encoder, KeyValueGroupedDataset }

class DatasetWithStateProcessingTimeout[K, V](g: KeyValueGroupedDataset[K, V], o: OutputMode) {
  def flatmapGroupsWithState[S: Encoder, U: Encoder](
    f: (K, List[V], State[S]) => (State[S], List[U])
  ): Dataset[U] = {
    val f2 = (s: FlatmapStateProcessingTimeout[K, V, S]) => s.flatMap(f)
    g.flatMapGroupsWithState[S, U](o, GroupStateTimeout.EventTimeTimeout()) { case (k, v, s) =>
      val ss = s.getOption.fold[State[S]](NoneState(s.getCurrentProcessingTimeMs())) { sv =>
        if (s.hasTimedOut) {
          TimedOutState(None, s.getCurrentProcessingTimeMs(), sv)
        } else {
          SomeState(None, s.getCurrentProcessingTimeMs(), sv)
        }
      }
      val q = f2(new FlatmapStateProcessingTimeout(k, v.toList, ss))
      q.state match {
        case NoneState(_)               => s.remove()
        case TimedOutState(None, _, ss) => s.update(ss)
        case SomeState(None, _, ss)     => s.update(ss)
        case TimedOutState(Some(d), _, ss) =>
          s.update(ss)
          s.setTimeoutDuration(d.toMillis)
        case SomeState(Some(d), _, ss) =>
          s.update(ss)
          s.setTimeoutDuration(d.toMillis)
      }
      q.values.iterator
    }
  }
}
