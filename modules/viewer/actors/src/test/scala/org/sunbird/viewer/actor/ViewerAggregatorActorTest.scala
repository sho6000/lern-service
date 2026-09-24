package org.sunbird.viewer.actor

import java.util
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.TestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sunbird.activity.util.HierarchyRelationsUtil
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.Response

import scala.concurrent.duration.FiniteDuration

/**
 * Unit tests for ViewerAggregatorActor guard branches (deterministic, no hierarchy fixture needed):
 *  - missing userId/courseId -> skip, still replies success.
 *  - no consumption rows -> early return, NO aggregate/enrolment writes.
 * The full recursive-rollup happy path depends on a published hierarchy_relations fixture and is
 * better exercised as an integration test; these guards pin the cheap-exit correctness.
 */
class ViewerAggregatorActorTest extends AnyFlatSpec with Matchers with MockFactory {

  val system: ActorSystem = ActorSystem.create("viewer-aggregator-test")

  private def emptyRows: Response = {
    val r = new Response(); r.put("response", new util.ArrayList[util.Map[String, AnyRef]]()); r
  }

  private def callActor(request: Request, props: Props): Response = {
    val probe = new TestKit(system)
    val actorRef = system.actorOf(props)
    actorRef.tell(request, probe.testActor)
    probe.expectMsgType[Response](FiniteDuration.apply(15, TimeUnit.SECONDS))
  }

  private def aggRequest(userId: String, courseId: String): Request = {
    val req = new Request
    req.setOperation("aggregate")
    if (userId != null) req.put("userId", userId)
    if (courseId != null) req.put("courseId", courseId)
    req.put("batchId", "b1")
    req
  }

  "aggregate" should "skip and reply success when userId/courseId are missing" in {
    val ops = mock[CassandraOperation]
    val hru = mock[HierarchyRelationsUtil]
    // no cassandra / hierarchy interaction expected on the missing-id guard
    val result = callActor(aggRequest(null, null), Props(new ViewerAggregatorActor().configure(ops, hru)))
    result should not be null
  }

  "aggregate" should "early-return with no writes when there is no consumption" in {
    val ops = mock[CassandraOperation]
    val hru = mock[HierarchyRelationsUtil]
    // readConsumption -> empty; must NOT reach batchUpdateWithPutAll / updateRecordV2
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    val result = callActor(aggRequest("u1", "c1"), Props(new ViewerAggregatorActor().configure(ops, hru)))
    result should not be null
  }

  // C2: contentstatus is a full-column replace in updateRecordV2, so the rollup must MERGE freshly computed
  // leaf statuses into the enrolment row's existing map — a root-keyed read that sees only some leaves must
  // not wipe the others. Tests the extracted pure merge directly (the full rollup is an integration concern).
  "mergeContentStatus" should "preserve existing leaves and add/overwrite the fresh ones" in {
    val existing = new util.HashMap[String, AnyRef]() {{
      put("leaf-a", Integer.valueOf(2)) // completed earlier, not in this pass
      put("leaf-b", Integer.valueOf(1)) // in-progress, gets overwritten below
    }}
    val fresh = Map[String, AnyRef]("leaf-b" -> Integer.valueOf(2), "leaf-c" -> Integer.valueOf(2))
    val merged = ViewerAggregatorActor.mergeContentStatus(existing, fresh)
    merged.get("leaf-a") shouldBe Integer.valueOf(2) // preserved (not clobbered)
    merged.get("leaf-b") shouldBe Integer.valueOf(2) // fresh wins on conflict
    merged.get("leaf-c") shouldBe Integer.valueOf(2) // added
    merged.size() shouldBe 3
  }

  "mergeContentStatus" should "tolerate a null existing map" in {
    val merged = ViewerAggregatorActor.mergeContentStatus(null, Map[String, AnyRef]("leaf-a" -> Integer.valueOf(1)))
    merged.get("leaf-a") shouldBe Integer.valueOf(1)
    merged.size() shouldBe 1
  }
}
