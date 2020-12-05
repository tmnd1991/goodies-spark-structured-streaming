package com.github.tmnd1991.spark.goodies.impl

import com.github.tmnd1991.spark.goodies.eventTimeout.State

final class FlatmapStateEventTimeout[K, V, S] (
                                                                 key: K,
                                                                 val values: List[V],
                                                                 val state: State[S]
                                                               ) {
  def flatMap[U](
                  f: (K, List[V], State[S]) => (State[S], List[U])
                ): FlatmapStateEventTimeout[K, U, S] = {
    val (nS, nV) = f(key, values, state)
    new FlatmapStateEventTimeout(key, nV, nS)
  }
  def flatMapState(f: (K, List[V], State[S]) => State[S]): FlatmapStateEventTimeout[K, V, S] =
    flatMap((k, v, s) => f(k, v, s) -> values)

  def flatMapValue[U](f: (K, List[V], State[S]) => List[U]): FlatmapStateEventTimeout[K, U, S] =
    flatMap((k, v, s) => s -> f(k, v, s))
}
