package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Created by wuxiaoqi on 17-11-30.
 */
case class HyperGraphVertex(vId: Int, equivalenceClass: Seq[Expression]) {

}
