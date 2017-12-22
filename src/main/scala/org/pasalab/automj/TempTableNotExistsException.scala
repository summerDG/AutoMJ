package org.pasalab.automj

import org.apache.spark.sql.AnalysisException

/**
 * Created by wuxiaoqi on 17-12-21.
 */
class TempTableNotExistsException(viewName: String)
  extends AnalysisException(s"Temporary table `$viewName` does not exists")
