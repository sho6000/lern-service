package org.sunbird.activity.util

import java.util

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.request.RequestContext
import org.sunbird.response.Response

/**
 * HierarchyRelationsUtil: the JVM-wide TTL cache on readFromDB.
 * Unique relationship keys per test avoid cross-test cache bleed.
 */
class HierarchyRelationsUtilTest extends AnyFlatSpec with Matchers with MockFactory {

  private def responseWith(nodeIds: util.List[String]): Response = {
    val row = new util.HashMap[String, AnyRef]() {{ put("node_ids", nodeIds) }}
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(row)
    val r = new Response(); r.put("response", rows); r
  }

  private def nodeList(ids: String*): util.List[String] = {
    val l = new util.ArrayList[String](); ids.foreach(l.add); l
  }

  // --- readFromDB TTL cache: repeat served from memory; empty NOT cached ---

  "getLeafNodes" should "hit the DB once and serve the repeat from cache" in {
    val ops = mock[CassandraOperation]
    (ops.getRecordsByProperties(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(responseWith(nodeList("leaf-A", "leaf-B"))).once()
    val u = HierarchyRelationsUtil(ops)
    val col = "cacheHit-collection-unique-1"
    val first = u.getLeafNodes(col, col, null)
    val second = u.getLeafNodes(col, col, null)
    first should contain allOf("leaf-A", "leaf-B")
    second shouldBe first
  }

  "readFromDB" should "not cache empty results (freshly-published collection is re-read)" in {
    val ops = mock[CassandraOperation]
    val emptyResp = new Response(); emptyResp.put("response", new util.ArrayList[util.Map[String, AnyRef]]())
    val populated = responseWith(nodeList("leaf-X"))
    (ops.getRecordsByProperties(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(emptyResp).once()
    (ops.getRecordsByProperties(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(populated).once()
    val u = HierarchyRelationsUtil(ops)
    val col = "negativeCache-collection-unique-2"
    u.getLeafNodes(col, col, null) shouldBe empty
    u.getLeafNodes(col, col, null) should contain("leaf-X")
  }
}
