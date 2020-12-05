package com.github.tmnd1991.spark.goodies.impl

import com.github.tmnd1991.spark.goodies.noTimeout.State

final class FlatmapStateNoTimeout[K, V, S](
  key: K,
  val values: List[V],
  val state: State[S]
) {
  def flatMap[U](
    f: (K, List[V], State[S]) => (State[S], List[U])
  ): FlatmapStateNoTimeout[K, U, S] = {
    val (nS, nV) = f(key, values, state)
    new FlatmapStateNoTimeout(key, nV, nS)
  }
  def flatMapState(f: (K, List[V], State[S]) => State[S]): FlatmapStateNoTimeout[K, V, S] =
    flatMap((k, v, s) => f(k, v, s) -> values)

  def flatMapValue[U](f: (K, List[V], State[S]) => List[U]): FlatmapStateNoTimeout[K, U, S] =
    flatMap((k, v, s) => s -> f(k, v, s))
}
