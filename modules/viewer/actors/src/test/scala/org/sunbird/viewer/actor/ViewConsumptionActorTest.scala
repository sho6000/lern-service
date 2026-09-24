package org.sunbird.viewer.actor

import java.util
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.testkit.TestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.Response

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
 * Unit tests for ViewConsumptionActor — view lifecycle (start/update/end/read). CassandraOperation
 * is mocked via the setCassandraOperation seam; the aggregator is a stub actor that replies
 * immediately so the sync ask in viewEnd returns fast.
 *
 * Every write op also calls touchEnrolmentAccess, which reads the enrolment (getRecordByIdentifier)
 * and only stamps it (updateRecordV2) when it EXISTS — so each test stubs that read. The two
 * "touchEnrolmentAccess" cases pin that guard (C1: no phantom enrolment row on an unenrolled view).
 */
class ViewConsumptionActorTest extends AnyFlatSpec with Matchers with MockFactory {

  implicit val ec: ExecutionContext = ExecutionContext.global
  val system: ActorSystem = ActorSystem.create("viewer-consumption-test")

  // stub aggregator: replies to the Patterns.ask so triggerAggregation completes without the 30s timeout
  private def replyingAggregator = system.actorOf(Props(new Actor {
    def receive: Receive = { case _ => sender() ! new Response() }
  }))

  private def emptyRows: Response = {
    val r = new Response(); r.put("response", new util.ArrayList[util.Map[String, AnyRef]]()); r
  }

  private def rowsWith(rows: util.List[util.Map[String, AnyRef]]): Response = {
    val r = new Response(); r.put("response", rows); r
  }

  private def uccRow(status: Int): util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]() {{
    put("userid", "u1"); put("collectionid", "c1"); put("contextid", "b1"); put("contentid", "ct1")
    put("status", Integer.valueOf(status))
  }}

  private def enrolmentRow: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]() {{
    put("userid", "u1"); put("courseId", "c1"); put("batchId", "b1"); put("status", Integer.valueOf(1))
  }}

  // touchEnrolmentAccess's enrolment read; `result` decides whether the row is stamped.
  private def stubEnrolmentRead(ops: CassandraOperation, result: Response) =
    (ops.getRecordByIdentifier(_: String, _: String, _: Object, _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(result)

  private def expectEnrolmentStamp(ops: CassandraOperation) =
    (ops.updateRecordV2(_: String, _: String, _: util.Map[String, AnyRef], _: util.Map[String, AnyRef], _: Boolean, _: RequestContext))
      .expects(*, *, *, *, *, *).returns(new Response()).once()

  private def callActor(request: Request, props: Props): Response = {
    val probe = new TestKit(system)
    val actorRef = system.actorOf(props)
    actorRef.tell(request, probe.testActor)
    probe.expectMsgType[Response](FiniteDuration.apply(15, TimeUnit.SECONDS))
  }

  private def viewRequest(op: String): Request = {
    val req = new Request
    req.setOperation(op)
    req.put("userId", "u1"); req.put("courseId", "c1"); req.put("batchId", "b1"); req.put("contentId", "ct1")
    req
  }

  "viewStart" should "insert a new ucc row when absent" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewStart"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  "viewStart" should "be a no-op when the row already exists" in {
    val ops = mock[CassandraOperation]
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(uccRow(1))
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(rowsWith(rows))
    // no upsertRecord expectation -> a call would fail the strict mock
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewStart"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  "viewStart" should "keep the higher existing progress on merge (monotonic)" in {
    val ops = mock[CassandraOperation]
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(new util.HashMap[String, AnyRef]() {{
      put("userid", "u1"); put("collectionid", "c1"); put("contextid", "b1"); put("contentid", "ct1")
      put("status", Integer.valueOf(1)); put("progress", Integer.valueOf(80))
    }})
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(rowsWith(rows))
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(where { (_: String, _: String, row: util.Map[String, AnyRef], _: RequestContext) => row.get("progress") == Integer.valueOf(80) })
      .returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val req = viewRequest("viewStart"); req.put("progress", Integer.valueOf(50)) // lower than existing 80
    val result = callActor(req, Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  // scenario 2: collection given, no context -> contextid(batchid) cascades to collectionid(courseid)
  "viewStart" should "store batchid=courseId when contextId is absent (scenario 2)" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(where { (_: String, _: String, row: util.Map[String, AnyRef], _: RequestContext) =>
        row.get("collectionid") == "c1" && row.get("contextid") == "c1" && row.get("contentid") == "ct1" })
      .returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val req = new Request; req.setOperation("viewStart")
    req.put("userId", "u1"); req.put("courseId", "c1"); req.put("contentId", "ct1")
    val result = callActor(req, Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  // scenario 1: individual content -> collectionid & contextid both collapse to contentId
  "viewStart" should "store courseid=batchid=contentId for individual content (scenario 1)" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(where { (_: String, _: String, row: util.Map[String, AnyRef], _: RequestContext) =>
        row.get("collectionid") == "ct1" && row.get("contextid") == "ct1" && row.get("contentid") == "ct1" })
      .returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val req = new Request; req.setOperation("viewStart")
    req.put("userId", "u1"); req.put("contentId", "ct1")
    val result = callActor(req, Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  "viewUpdate" should "upsert when the row exists and is not completed" in {
    val ops = mock[CassandraOperation]
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(uccRow(1))
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(rowsWith(rows))
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewUpdate"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  "viewEnd" should "write status=2 and trigger the aggregation" in {
    val ops = mock[CassandraOperation]
    // viewEnd reads the existing ucc row to merge viewcount/lastCompletedTime monotonically
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewEnd"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  // C1: touchEnrolmentAccess must NOT fabricate an enrolment for an unenrolled/no-context view.
  "touchEnrolmentAccess" should "not stamp the enrolment when none exists" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows) // no enrolment
    // no updateRecordV2 expectation -> a stamp write would fail the strict mock (phantom-row guard)
    val result = callActor(viewRequest("viewEnd"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  // C1: when the enrolment DOES exist, its last-access is stamped exactly once.
  "touchEnrolmentAccess" should "stamp the enrolment when it exists" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(enrolmentRow)
    stubEnrolmentRead(ops, rowsWith(rows)) // enrolment present
    expectEnrolmentStamp(ops)
    val result = callActor(viewRequest("viewEnd"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result should not be null
  }

  // spec: result = { userId, contentId, type:"content", contents:[{ collectionId, contextId, contentId, status, progressDetails{…} }] }
  "viewRead" should "return the spec-shaped result (contents wrapper, spec keys, nested progressDetails)" in {
    val ops = mock[CassandraOperation]
    // production casing: CassandraUtil camelCases mapped columns (contentId/viewCount/lastAccessTime) but leaves collectionid/contextid/progressdetails lowercase.
    // legacy content-state columns (dateTime/oldLast*/completedCount/completionPercentage) share the table but must NOT leak into view.read.
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(new util.HashMap[String, AnyRef]() {{
      put("userId", "u1"); put("collectionid", "c1"); put("contextid", "b1"); put("contentId", "ct1")
      put("status", Integer.valueOf(2)); put("progress", Integer.valueOf(100))
      put("progressdetails", "{\"mimeType\":\"application/video\",\"progress\":100}")
      put("viewCount", Integer.valueOf(3)); put("lastAccessTime", new java.util.Date(1000L))
      put("dateTime", new java.util.Date(1L)); put("addedBy", "u9"); put("oldLastAccessTime", "2020-01-01")
      put("completedCount", Integer.valueOf(5)); put("completionPercentage", java.lang.Float.valueOf(50.0f))
    }})
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(rowsWith(rows))
    val result = callActor(viewRequest("viewRead"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    val res = result.getResult
    res.get("type") shouldBe "content"
    res.get("userId") shouldBe "u1"
    res.get("collectionId") shouldBe "c1"
    res.get("contextId") shouldBe "b1"
    res.containsKey("response") shouldBe false
    val contents = res.get("contents").asInstanceOf[util.List[util.Map[String, AnyRef]]]
    contents.size() shouldBe 1
    val item = contents.get(0)
    item.get("collectionId") shouldBe "c1"
    item.get("contextId") shouldBe "b1"
    item.get("contentId") shouldBe "ct1"
    item.get("status") shouldBe Integer.valueOf(2)
    item.get("progressDetails").isInstanceOf[util.Map[_, _]] shouldBe true  // object, not JSON string
    item.get("progress") shouldBe Integer.valueOf(100)                      // viewer-owned fields kept
    item.get("viewCount") shouldBe Integer.valueOf(3)
    item.containsKey("userId") shouldBe false                               // already top-level, not repeated per item
    // kept: completedCount/completionPercentage/dateTime/addedBy pass through
    item.get("completedCount") shouldBe Integer.valueOf(5)
    item.get("completionPercentage") shouldBe java.lang.Float.valueOf(50.0f)
    item.containsKey("dateTime") shouldBe true
    item.get("addedBy") shouldBe "u9"
    // dropped: only the old_* migration columns
    item.containsKey("oldLastAccessTime") shouldBe false
  }

  // spec: result = { userId, contentId, collectionId, contextId, assessments:[{ attemptId, score, max_score }] } (per attempt)
  "assessmentRead" should "return spec-shaped assessments (attemptId per attempt, collectionId/contextId)" in {
    val ops = mock[CassandraOperation]
    val attempts = new util.ArrayList[util.Map[String, AnyRef]]()
    attempts.add(new util.HashMap[String, AnyRef]() {{
      put("attempt_id", "a1"); put("content_id", "ct1")
      put("total_score", java.lang.Double.valueOf(6.0)); put("total_max_score", java.lang.Double.valueOf(10.0))
    }})
    attempts.add(new util.HashMap[String, AnyRef]() {{
      put("attempt_id", "a2"); put("content_id", "ct1")
      put("total_score", java.lang.Double.valueOf(8.0)); put("total_max_score", java.lang.Double.valueOf(10.0))
    }})
    (ops.getRecordsByProperties(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(rowsWith(attempts))
    val result = callActor(viewRequest("assessmentRead"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    val res = result.getResult
    res.get("userId") shouldBe "u1"
    res.get("collectionId") shouldBe "c1"
    res.get("contextId") shouldBe "b1"
    res.containsKey("courseId") shouldBe false
    val assessments = res.get("assessments").asInstanceOf[util.List[util.Map[String, AnyRef]]]
    assessments.size() shouldBe 2
    val ids = assessments.asScala.map(_.get("attemptId")).toSet
    ids shouldBe Set("a1", "a2")
    val a2 = assessments.asScala.find(_.get("attemptId") == "a2").get
    a2.get("score").asInstanceOf[Double] shouldBe 8.0
    a2.get("max_score").asInstanceOf[Double] shouldBe 10.0
  }

  // spec acks: view.start/update/end put { contentId -> "<phrase>" } into result
  "viewStart" should "ack with the spec phrase keyed by contentId" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewStart"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result.getResult.get("ct1") shouldBe "Progress started"
  }

  "viewUpdate" should "ack with the spec phrase keyed by contentId" in {
    val ops = mock[CassandraOperation]
    val rows = new util.ArrayList[util.Map[String, AnyRef]](); rows.add(uccRow(1))
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(rowsWith(rows))
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewUpdate"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result.getResult.get("ct1") shouldBe "Progress Updated"
  }

  "viewEnd" should "ack with the spec phrase keyed by contentId" in {
    val ops = mock[CassandraOperation]
    (ops.getRecords(_: String, _: String, _: util.Map[String, AnyRef], _: util.List[String], _: RequestContext))
      .expects(*, *, *, *, *).returns(emptyRows)
    (ops.upsertRecord(_: String, _: String, _: util.Map[String, AnyRef], _: RequestContext))
      .expects(*, *, *, *).returns(new Response()).once()
    stubEnrolmentRead(ops, emptyRows)
    val result = callActor(viewRequest("viewEnd"),
      Props(new ViewConsumptionActor(replyingAggregator).setCassandraOperation(ops)))
    result.getResult.get("ct1") shouldBe "Progress ended"
  }
}
