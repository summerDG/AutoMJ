package org.pasalab.automj

/**
  * Created by summerDG on 2017/11/30.
  */
class GHD(children: Seq[GHD]) {

}
object GHD {
  def apply(hyperGraph: HyperGraph): GHD = {
    new GHD(Seq[GHD]())
  }
}
