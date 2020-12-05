# goodies-spark-structured-streaming

This project aims to help you write more concise and correct spark stateful queries.

## Accessing the library

From sbt:

```scala
libraryDependencies += "com.github.tmnd1991" %% "goodies-spark-structured-streaming" % "0.0.1"
```

From maven (2.12):

```xml
<dependency>
    <groupId>com.github.tmnd1991</groupId>
    <artifactId>testing-spark-structured-streaming_2.12</artifactId>
    <version>0.0.1</version>
</dependency>
```

From maven (2.11):

```xml
<dependency>
    <groupId>com.github.tmnd1991</groupId>
    <artifactId>testing-spark-structured-streaming_2.11</artifactId>
    <version>0.0.1</version>
</dependency>
```

And if you want to depend on some snapshot version, you need to add sonatype snapshot repository:

On sbt:

```scala
resolvers += Resolver.sonatypeRepo("snapshots")
```

On maven:

```xml
<repositories>
  <repository>
    <id>snapshots-repo</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <releases><enabled>false</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>
```
## Documentation

This library differentiates Spark streaming stateful queries "modes" at the type level.

### No Timeout

```scala
import com.github.tmnd1991.spark.goodies.noTimeout._
df.groupByKey(_.toString.head.toString)
  .outputMode(OutputMode.Update())
  .flatmapGroupsWithState{
    case (key, values, s@NoneState(currentTs)) => s -> List("")
    case (key, values, s@SomeState(currentTs,stateContent)) => s -> List("")
}
```

### Event Timeout

```scala
import com.github.tmnd1991.spark.goodies.eventTimeout._
df.groupByKey(_.toString.head.toString)
  .outputMode(OutputMode.Update())
  .flatmapGroupsWithState[Boolean, String] {
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
```

### Processing Timeout

```scala
import com.github.tmnd1991.spark.goodies.processingTimeout._
df.groupByKey(_.toString.head.toString)
  .outputMode(OutputMode.Update())
  .flatmapGroupsWithState[Boolean, String] {
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
```
## How to contribute

Want to contribute? Open a PR 😀