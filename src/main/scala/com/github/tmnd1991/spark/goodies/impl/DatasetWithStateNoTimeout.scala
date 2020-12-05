package com.github.tmnd1991.spark.goodies.impl

import com.github.tmnd1991.spark.goodies.noTimeout.{ NoneState, SomeState, State }
import org.apache.spark.sql.streaming.{ GroupStateTimeout, OutputMode }
import org.apache.spark.sql.{ Dataset, Encoder, KeyValueGroupedDataset }

class DatasetWithStateNoTimeout[K, V](g: KeyValueGroupedDataset[K, V], o: OutputMode) {
  def flatmapGroupsWithState[S: Encoder, U: Encoder](
    f: (K, List[V], State[S]) => (State[S], List[U])
  ): Dataset[U] = {
    val f2 = (s: FlatmapStateNoTimeout[K, V, S]) => s.flatMap(f)
    g.flatMapGroupsWithState[S, U](o, GroupStateTimeout.NoTimeout()) { case (k, v, s) =>
      val ss = s.getOption.fold[State[S]](NoneState(s.getCurrentProcessingTimeMs()))(
        SomeState(s.getCurrentProcessingTimeMs(), _)
      )
      val q = f2(new FlatmapStateNoTimeout(k, v.toList, ss))
      q.state match {
        case NoneState(_)     => s.remove()
        case SomeState(_, ss) => s.update(ss)
      }
      q.values.iterator
    }
  }
}
