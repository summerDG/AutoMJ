package org.pasalab.automj

/**
  * Created by summerDG on 2017/11/30.
  */
class GHD(children: Seq[GHD]) {
  def logGTA: GHD = {

  }
}
object GHD {
  def apply(hyperGraph: HyperGraph): GHD = {
    // 生成初始化的GHD, 宽度为1
    new GHD(Seq[GHD]())
  }
}
