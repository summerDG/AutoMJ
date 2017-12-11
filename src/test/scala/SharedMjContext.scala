import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-9.
 */
trait SharedMjContext extends SharedSQLContext{
  val conf = sessio
}
