package org.pasalab.automj

// [a,b,c]|Share|100
class ConfigTreeNode(name: String) {
  private val pair = name.split('|')
  assert(pair.length == 3, s"format is wrong, $name, should be [a,b,c]|Share|100")
  val output: Seq[String] = {
    val str = pair(0)
    str.substring(1, str.length - 1).split(',')
  }
  val joinType = pair(1)
  val size = pair(2).toLong
}
