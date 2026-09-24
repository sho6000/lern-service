package org.sunbird.viewer.actor

import java.util
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.TestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.exception.ProjectCommonException
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.Response

import scala.concurrent.duration.FiniteDuration

// Summary APIs — spec shapes: summary.read = single enriched object, summary.list = { summary:[…] },
// summary.delete = { userId -> ack }. Collection/assessment enrichment is fail-safe (empty here, no content service).
class ViewerSummaryActorTest extends AnyFlatSpec with Matchers with MockFactory {

  val system: ActorSystem = ActorSystem.create("viewer-summary-test")

  private def rowsResp(rs: util.List[util.Map[String, AnyRef]]): Response = {
    val r = new Response; r.put("response", rs); r
  }

  private def enrolmentRows: util.List[util.Map[String, AnyRef]] = {
    val l = new util.ArrayList[util.Map[String, AnyRef]]()
    l.add(new util.HashMap[String, AnyRef]() {{
      put("userId", "u1"); put("courseId", "c1"); put("batchId", "b1")
      put("status", Integer.valueOf(2)); put("progress", Integer.valueOf(100))
      put("contentStatus", new util.HashMap[String, AnyRef]() {{ put("ct1", Integer.valueOf(2)) }})
    }})
    l
  }

  private def anyGetRecords(ops: CassandraOperation, resp: Response) =
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returning(resp).anyNumberOfTimes()

  private def callActor(request: Request, ops: CassandraOperation): Response = {
    val probe = new TestKit(system)
    val ref = system.actorOf(Props(new ViewerSummaryActor().setCassandraOperation(ops)))
    ref.tell(request, probe.testActor)
    probe.expectMsgType[Response](FiniteDuration.apply(15, TimeUnit.SECONDS))
  }

  "summaryRead" should "return a single enriched object (spec keys, no list wrapper)" in {
    val ops = mock[CassandraOperation]
    anyGetRecords(ops, rowsResp(enrolmentRows))
    val req = new Request; req.setOperation("summaryRead")
    req.put("userId", "u1"); req.put("courseId", "c1"); req.put("batchId", "b1")
    val res = callActor(req, ops).getResult
    res.get("collectionId") shouldBe "c1"
    res.get("contextId") shouldBe "b1"
    res.get("status") shouldBe Integer.valueOf(2)
    res.containsKey("response") shouldBe false
    res.containsKey("summary") shouldBe false
    res.get("contentStatus").asInstanceOf[util.Map[String, AnyRef]].get("ct1") shouldBe Integer.valueOf(2)
  }

  "summaryList" should "wrap enrolments under summary[]" in {
    val ops = mock[CassandraOperation]
    anyGetRecords(ops, rowsResp(enrolmentRows))
    val req = new Request; req.setOperation("summaryList"); req.put("userId", "u1")
    val res = callActor(req, ops).getResult
    val summary = res.get("summary").asInstanceOf[util.List[util.Map[String, AnyRef]]]
    summary.size() shouldBe 1
    summary.get(0).get("collectionId") shouldBe "c1"
  }

  "summaryRead" should "reject when contextId is missing (collectionId + contextId are mandatory)" in {
    val ops = mock[CassandraOperation] // no getRecords expected — validation throws first
    val req = new Request; req.setOperation("summaryRead"); req.put("userId", "u1"); req.put("courseId", "c1")
    val probe = new TestKit(system)
    val ref = system.actorOf(Props(new ViewerSummaryActor().setCassandraOperation(ops)))
    ref.tell(req, probe.testActor)
    probe.expectMsgType[ProjectCommonException](FiniteDuration.apply(15, TimeUnit.SECONDS))
  }

  "summaryDelete" should "purge multiple tables (not just the enrolment) and ack keyed by userId" in {
    val ops = mock[CassandraOperation]
    val tables = scala.collection.mutable.Set[String]()
    (ops.deleteRecord(_: String, _: String, _: util.Map[String, String], _: RequestContext))
      .expects(*, *, *, *).onCall { (_: String, t: String, _: util.Map[String, String], _: RequestContext) => tables += t; () }
      .anyNumberOfTimes()
    val req = new Request; req.setOperation("summaryDelete")
    req.put("userId", "u1"); req.put("courseId", "c1"); req.put("batchId", "b1")
    val res = callActor(req, ops).getResult
    res.get("u1") shouldBe "Enrolment Deleted Succesfully"
    res.get("purged").asInstanceOf[util.List[util.Map[String, AnyRef]]].size() shouldBe 1
    tables.size should be >= 3 // enrolment + consumption + assessment (+ activity_agg when configured)
  }
}
