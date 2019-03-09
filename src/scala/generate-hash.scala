import java.io.File
import java.nio.charset.Charset
import java.text.DecimalFormat

import com.google.common.hash.Hashing
import com.google.common.io.Files

/*
Compile with scalac then
scala Main
or
to run the code as java
java -cp /Applications/scala-2.9.1.final/lib/scala-library.jar Main

scala -classpath .:`pwd`/libs/guava-11.0.1.jar src/scala/generate-hash.scala /Users/hujol/Desktop
*/


val s = new Array[String](1)
s(0) = "Preparing the hash per file"

println(s(0))

//val hf = Hashing.md5()
val hf = Hashing.sha1() // Slightly slower 1.5 for 1.4 (7% slower)

val oriDir = new File(args(0))
//for(d <- oriDir.listFiles() if d.isDirectory; f <- d.listFiles(); n = f.getName)

val utf8 = Charset.forName("utf-8")
val currentTime35: Long = System.currentTimeMillis
for(f <- oriDir.listFiles() if f.isFile; n = f.getName) {
  val t = hf.hashString(Files.toString(f, utf8), utf8).toString + "," + f
  Files.append("\n" + t, new File("/tmp/hashfiles/1.txt"), utf8)
  println(t)
}
val delta35: Long = System.currentTimeMillis - currentTime35
System.out.println("**** Executed test in " + new DecimalFormat("0.0000").format(delta35 / 1000f) + "s")
