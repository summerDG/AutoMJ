package org.pasalab.automj

import java.io.{ByteArrayOutputStream, IOException, ObjectOutputStream}

/**
  * Created by summerDG on 2018/3/1.
  */
object ByteArrayUtils {
  def objectToBytes[T](obj: T): Option[Array[Byte]] = {
    var bytes: Array[Byte] = null
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    var sOut: ObjectOutputStream= null
    try {
      sOut = new ObjectOutputStream(out)
      sOut.writeObject(obj)
      sOut.flush()
      bytes = out.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    if (bytes == null) None else Some(bytes)
  }
}
