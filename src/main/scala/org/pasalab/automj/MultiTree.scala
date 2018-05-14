package org.pasalab.automj

case class MultiTree[T](v: T, children: Seq[MultiTree[T]])