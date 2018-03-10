package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.mutable
/**
 * Created by wuxiaoqi on 17-12-1.
 */
class Graph[T](v: Seq[Int], e: Seq[(Int, Int)], nodeToId: Map[Node[T], Int]) {
  def V: Int = v.length
  def E: Int = e.length
  def adj(n: Int): Seq[Int] = {
    e.flatMap{
      case (n1, n2) =>
        if (n1 == n) Some(n2)
        else if (n2 == n) Some(n1)
        else None
    }
  }
  /**
   * 找图中的强联通分量
   * @return 强联通分量
   */
  def connectComponent(): Seq[Seq[Node[T]]] = {
    val G = this

    val marked = new Array[Boolean](G.V)
    val id = new Array[Int](G.V).map(_ => -1)
    var count = 0

    def dfs(v: Int): Unit = {
      marked(v) = true
      id(v) = count
      for (w <- G.adj(v)) {
        if (!marked(w)) dfs(w)
      }
    }

    assert(G.V > 0, s"nodes is ${G.V}")
    var s = 0
    while (s < G.V) {
      if (!marked(s)) {
        dfs(s)
        count += 1
      }
      s += 1
    }
    assert(id.forall(_ >= 0), s"some(${id.filter(_ < 0)}) node without id")
    assert(count > 0, s"count is $count")
    val idToNode = nodeToId.map(x => (x._2, x._1))
    val closures = new Array[mutable.ArrayBuffer[Node[T]]](count)

    var i = 0
    while (i < G.V) {
      val node = idToNode(i)
      val cId: Int = id(i)
      if (closures(cId) == null) {
        closures(cId) = mutable.ArrayBuffer(node)
      } else {
        closures(cId) += node
      }
      i += 1
    }
    assert(closures.nonEmpty && closures.forall(_.nonEmpty),
      s"connectComponent has empty member(closure empty: ${closures.isEmpty}, all is empty: ${closures.forall(_.isEmpty)}," +
        s" ${closures.zipWithIndex.filter(_._1.isEmpty).map(_._2).mkString(",")})")
    closures
  }
}
object Graph {
  /**
   * 通过join关系构造图
   * @param m (left relation id, right relation id) -> join condition
   * @return 图
   */
  def apply[T](m: Seq[(T, T)]): Graph[T] = {
    assert(m.nonEmpty, s"input map is empty")
    val (nodePair, edges) = m.map {
      case (l, r) =>
        val lNode = Node(l)
        val rNode = Node(r)
        val edge = Edge(lNode, rNode)

        ((lNode, rNode), edge)
    }.unzip
    val nodes = nodePair.flatMap(x => Seq[Node[T]](x._1, x._2))
    assert(nodes.length > 0, s"nodes is empty")
    val nodeToId = nodes.toSet[Node[T]].zipWithIndex.toMap
    assert(nodeToId.nonEmpty, s"nodeToId is Empty")
    val v = nodeToId.unzip._2.toSeq
    val e = edges.map(e => (nodeToId(e.n1), nodeToId(e.n2))).toSet.toSeq

    new Graph(v, e, nodeToId)
  }
  def cIdTreeToRIdTree(root: MultipleJoinTreeNode,
                       joinedRIds: mutable.Set[Int],
                       initEdges: Map[(Int, Int), Set[Int]]): MultipleJoinTreeNode = {
    val (leaves, nonLeaves) = root.children.partition(c => c.children == null || c.children.isEmpty)
    val nonFreshLeaves = nonLeaves.map(n => cIdTreeToRIdTree(n, joinedRIds, initEdges))
    val cIds = leaves.map (_.v)
    val rIds = mutable.Set[Int]()
    for (((a,b), ids) <- initEdges) {
      if (cIds.contains(a) && cIds.contains(b)) {
        rIds ++= ids.filter(x => !joinedRIds.contains(x))
      }
    }
    joinedRIds ++= rIds
    val freshLeaves = rIds.map(r => MultipleJoinTreeNode(r, null))

    MultipleJoinTreeNode(joinedRIds.min, nonFreshLeaves ++ freshLeaves)
  }
  //TODO: 由于每次找到的最大团不一定与之前的join节点有关, 所以不一定是左深度树
//  def transformToJoinTree(nodesCount: Int, relationNum: Int,
//                          initEdges: Map[(Int, Int), Set[Int]],
//                          rIdToCIds: Seq[Set[Int]]): MultipleJoinTreeNode = {
//    val cIdTree = transformToCidTree(nodesCount, initEdges, rIdToCIds)
//    val joinedRIds = mutable.Set[Int]()
//    val tree = cIdTreeToRIdTree(cIdTree, joinedRIds, initEdges)
//    assert(joinedRIds.size == relationNum, s"There exists relation which not in join tree!!!")
//    tree
//  }
  //TODO: 由于每次找到的最大团不一定与之前的join节点有关, 所以不一定是左深度树
  def transformToJoinTree(nodesCount: Int,
                          initEdges: Map[(Int, Int), Set[Int]],
                          rIdToCids: Seq[Set[Int]]): MultipleJoinTreeNode = {
    // 最新的查询结构图, 每一轮操作要更新
    var edges: Seq[(Int, Int)] = initEdges.toSeq.map(_._1)
    // 记录被添加到树中的表
    val scanned: mutable.Set[Int] = new mutable.HashSet[Int]()
    var root: MultipleJoinTreeNode = null
    // 第一个value为对应的节点, 第二个value代表其包含的变量集合
    val nodeIdToCids: mutable.HashMap[Int,(MultipleJoinTreeNode, Set[Int])] =
      new mutable.HashMap[Int,(MultipleJoinTreeNode, Set[Int])]()
    do {
      val m = new MaximalCliquesWithPivot(nodesCount, edges)
      m.Bron_KerboschPivotExecute()

      val maxClique = {
        val cliques = m.getMaxCliques
        // 这里由于scanned的保证，所以cliques永远不为空
        // 尽量让当前的最大团与之前的Join节点有关联, 因为在之前的试验中，有些时候把语法树并行度增加了之后性能反而会下降
        val maxLen = cliques.map(_.length).max
        val candidates = cliques.filter(_.length == maxLen)
        val preRootCandidate = candidates.filter(x => x.find(n => nodeIdToCids.contains(n.v)).isDefined)
        if (preRootCandidate.isEmpty) {
          candidates.head
        } else {
          preRootCandidate.head
        }
      }

      // 当前节点包含哪些变量
      val vIds = maxClique.flatMap {
        case Node(v) =>
          if (nodeIdToCids.contains(v)) nodeIdToCids(v)._2
          else Seq(v)
      }.toSet
      // 选取属于这个变量集合的表集合, 这些表不能被扫描过
      val rIds = (0 to rIdToCids.length - 1).filter(i => !scanned.contains(i) && rIdToCids(i).subsetOf(vIds)).toArray
      // 选取属于这个变量集合的节点集合
      val nodeIds = nodeIdToCids.filter{
        case (_, (_, v)) => v.subsetOf(vIds)
      }.map(_._1).toArray
      val nodeId = maxClique.map(_.v).min
      // 如果节点已经存在, 就不需要重新构造
      root = MultipleJoinTreeNode(nodeId,
        nodeIds.map(i => nodeIdToCids(i)._1) ++ rIds.map(x => MultipleJoinTreeNode(x, null)))

      // 1. 移除已经使用过的节点
      for (i <- nodeIds.sortWith(_ > _)) nodeIdToCids.remove(i)
      // 2. 更新(合并这轮使用的)节点
      nodeIdToCids.put(nodeId, (root, vIds))
      // 更新扫描过的表集合
      for (n <- rIds) {
        scanned.add(n)
      }
      edges = edges.map {
        case (l: Int, r: Int) =>
          val nl = if (vIds.contains(l)) nodeId else l
          val nr = if (vIds.contains(r)) nodeId else r
          (nl, nr)
      }.filter(x => x._1 != x._2).toSet.toSeq
    } while (scanned.size < rIdToCids.size)
    assert(nodeIdToCids.size == 1, s"Some node are not added in join tree")
    assert(rIdToCids.length == scanned.size, "There exists relation which not in join tree!!!")
    root
  }
}
case class Edge[T](n1: Node[T], n2: Node[T])
