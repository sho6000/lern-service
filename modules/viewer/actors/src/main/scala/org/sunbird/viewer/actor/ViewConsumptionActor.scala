package org.sunbird.viewer.actor

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.pekko.actor.ActorRef
import org.sunbird.assessment.models._
import org.sunbird.assessment.service.{AssessmentService, CassandraService, ContentService, KafkaService}
import org.sunbird.assessment.util.AssessmentParser
import org.sunbird.common.ProjectUtil
import org.sunbird.enrolments.BaseEnrolmentActor
import org.sunbird.exception.ProjectCommonException
import org.sunbird.helper.ServiceFactory
import org.sunbird.keys.JsonKey
import org.sunbird.learner.util.Util
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.{Response, ResponseCode}

import java.util
import javax.inject.{Inject, Named}
import scala.collection.JavaConverters._

// View lifecycle writes to user_content_consumption: read-modify-upsert with monotonic merge, per-userId serialized (not LWT).
class ViewConsumptionActor @Inject() (
    @Named("viewer-aggregator-actor") viewerAggregatorActor: ActorRef
) extends BaseEnrolmentActor {

  private val mapper = new ObjectMapper
  private var cassandraOperation = ServiceFactory.getInstance
  private val consumptionDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB)
  private val CONSUMPTION_TABLE = "user_content_consumption"
  private val enrolmentDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB)
  // assessment scoring reuses the assessment-aggregator services in-process (same math + persistence as the legacy job);
  // assessmentCassandra shares the injected CassandraOperation so setCassandraOperation() controls it in tests too
  private lazy val assessmentService = new AssessmentService(new ContentService())
  private lazy val assessmentCassandra = new CassandraService(Some(cassandraOperation))
  private lazy val kafkaService = new KafkaService()

  private def touchEnrolmentAccess(key: util.HashMap[String, AnyRef], status: Int, ctx: RequestContext): Unit = {
    val selectMap = new util.HashMap[String, AnyRef]() {{
      put("userid", key.get("userid")); put("courseid", key.get("collectionid")); put("batchid", key.get("contextid"))
    }}
    // only stamp an EXISTING enrolment — a plain UPDATE would fabricate a phantom row for an unenrolled view
    val existing = cassandraOperation.getRecordByIdentifier(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, selectMap, null, ctx)
      .getResult.getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]]).asInstanceOf[util.List[util.Map[String, AnyRef]]]
    if (existing.isEmpty) { logger.info(ctx, s"view: access skip(no-enrolment) | user=${key.get("userid")} course=${key.get("collectionid")} batch=${key.get("contextid")}"); return }
    val updateMap = new util.HashMap[String, AnyRef]() {{
      put("lastcontentaccesstime", new java.util.Date())
      put("lastreadcontentid", key.get("contentid"))
      put("lastreadcontentstatus", Integer.valueOf(status))
    }}
    cassandraOperation.updateRecordV2(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, selectMap, updateMap, true, ctx)
    logger.info(ctx, s"view: access stamped | user=${key.get("userid")} course=${key.get("collectionid")} content=${key.get("contentid")} status=$status")
  }

  override def onReceive(request: Request): Unit = {
    request.getOperation match {
      case "viewStart"      => viewStart(request)
      case "viewUpdate"     => viewUpdate(request)
      case "viewEnd"        => viewEnd(request)
      case "viewRead"       => viewRead(request)
      case "viewAssess"     => viewAssess(request)
      case "assessmentRead" => assessmentRead(request)
      case _                => onReceiveUnsupportedOperation(request.getOperation)
    }
  }

  // /v1/assessment/submit: score the attempt via the assessment services (same as legacy AssessmentAggregatorActor),
  // then treat the assessment leaf as completed content -> ucc status=2 + the same rollup as viewEnd.
  private def viewAssess(request: Request): Unit = {
    val ctx = request.getRequestContext
    val key = viewKey(request)
    val userId = key.get("userid").asInstanceOf[String]
    val collectionId = key.get("collectionid").asInstanceOf[String]
    val contextId = key.get("contextid").asInstanceOf[String]
    val contentId = key.get("contentid").asInstanceOf[String]

    val eventsRaw = Option(request.get(JsonKey.ASSESSMENT_EVENTS)).orElse(Option(request.get(JsonKey.EVENTS)))
      .map(_.asInstanceOf[util.List[util.Map[String, AnyRef]]]).getOrElse(new util.ArrayList[util.Map[String, AnyRef]]())
    val events: List[AssessmentEvent] = eventsRaw.asScala.map(AssessmentParser.mapToEvent).toList
    // no events -> nothing to score; refuse to complete the leaf (legacy never completed an assessment with no events)
    if (events.isEmpty)
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, "assessments (events) are required")

    // assessmentTs is a String in the content-state contract; accept Number, numeric String, or ISO-8601
    val ts = Option(request.get(JsonKey.ASSESSMENT_TS)).orElse(Option(request.get("assessmentTimestamp")))
      .flatMap(parseMillis).getOrElse(System.currentTimeMillis())
    // deterministic id (matches legacy generateId) so a replay of the same attempt upserts instead of double-counting
    val attemptId = Option(request.get(JsonKey.ATTEMPT_ID)).map(_.toString).filter(StringUtils.isNotBlank)
      .getOrElse(s"${userId}_${contentId}_$ts".hashCode.abs.toString)
    val unique = assessmentService.getUniqueQuestions(events)
    // same content-validation + skip-missing guards AssessmentAggregatorActor applies before scoring (both config-gated)
    val metadata = assessmentService.getMetadata(collectionId, contentId, ctx)
    if (!assessmentService.validateContent(AssessmentRequest(attemptId, userId, collectionId, contextId, contentId, ts, events), metadata))
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidRequestData, s"contentId $contentId is not valid for $collectionId")
    val skipMissing = Option(ProjectUtil.getConfigValue("assessment_skip_missing_records")).getOrElse("true").toBoolean
    if (skipMissing && metadata.totalQuestions > 0 && unique.size > metadata.totalQuestions)
      ProjectCommonException.throwClientErrorException(ResponseCode.invalidRequestData,
        s"unique events (${unique.size}) exceed total questions (${metadata.totalQuestions}) for $contentId")
    val metrics = assessmentService.computeScoreMetrics(unique)
    // collectionId/contextId are the resolved ucc-key values; CassandraService maps them to collection_id/context_id
    val result = AssessmentResult(attemptId, userId, collectionId, contextId, contentId,
      metrics.totalScore, metrics.totalMaxScore, metrics.grandTotal, metrics.questions, System.currentTimeMillis(), ts)
    assessmentCassandra.saveAssessment(result, ctx)
    // user_activity_agg keys context_id as "cb:"+batchId; only aggregate when a real batchId was supplied (else the row is an orphan)
    if (ViewerRequestKeys.batchId(request).isDefined) {
      val stored = assessmentCassandra.getUserAssessments(userId, collectionId, contextId, contentId, ctx)
      val agg = assessmentService.computeUserAggregates(userId, collectionId, contextId, stored)
      assessmentCassandra.updateUserActivity(userId, collectionId, contextId, agg, ctx)
      // same certificate trigger as AssessmentAggregatorActor.processUserAggregates (config-gated), so an
      // assessment-gated course issues certs on viewer-driven completion exactly as content-state does
      if (ProjectUtil.getConfigValue("assessment_aggregator_publish_certificate") == "true")
        kafkaService.publishCertificateEvent(userId, collectionId, contextId, assessmentService.getLatestAttemptId(agg))
    }

    val row = new util.HashMap[String, AnyRef](key)
    row.put("status", Integer.valueOf(2))
    row.put("progress", Integer.valueOf(100)) // completed assessment leaf, same as viewEnd
    row.put("last_completed_time", ProjectUtil.getTimeStamp)
    row.put("last_updated_time", ProjectUtil.getTimeStamp)
    cassandraOperation.upsertRecord(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE, row.asInstanceOf[util.Map[String, AnyRef]], ctx)
    touchEnrolmentAccess(key, 2, ctx)
    triggerAggregation(request, ctx)
    sender().tell(ackResponse(contentId, "Score Updated"), self)
  }

  private def ackResponse(contentId: String, message: String): Response = {
    val out = new Response(); out.put(contentId, message); out
  }

  // /v1/assessment/read: all attempts (score/max_score) per content from assessment_aggregator
  private def assessmentRead(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val courseIdOpt = ViewerRequestKeys.courseId(request)
    val batchIdOpt = ViewerRequestKeys.batchId(request)
    val contentIds: List[String] = request.get("contentId") match {
      case l: util.List[_] => l.asScala.map(_.asInstanceOf[String]).toList
      case s: String if StringUtils.isNotBlank(s) => List(s)
      case _ => List.empty
    }
    // assessments are per-attempt (not best-per-content), each tagged with its contentId
    val assessments = new util.ArrayList[util.Map[String, AnyRef]]()
    contentIds.foreach { cid =>
      // same cascade viewAssess wrote with: collectionid <- courseId?:content, contextid <- batchId?:courseId?:content
      val collectionId = courseIdOpt.getOrElse(cid)
      val contextId = batchIdOpt.orElse(courseIdOpt).getOrElse(cid)
      val stored = assessmentCassandra.getUserAssessments(userId, collectionId, contextId, cid, ctx)
      stored.foreach { a =>
        val m = new util.HashMap[String, AnyRef]()
        m.put("contentId", cid)
        m.put("attemptId", a.attemptId)
        m.put("score", a.totalScore.asInstanceOf[AnyRef])
        m.put("max_score", a.totalMaxScore.asInstanceOf[AnyRef])
        assessments.add(m)
      }
    }
    val out = new Response()
    out.put(JsonKey.USER_ID, userId)
    out.put("collectionId", courseIdOpt.orNull)
    out.put("contextId", batchIdOpt.orNull)
    out.put("assessments", assessments)
    sender().tell(out, self)
  }

  // resolves partial keys like viewKey: collectionid <- courseId?:content, contextid <- batchId?:courseId?:content; context=all ignores contextid
  private def viewRead(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val allContexts = "all".equalsIgnoreCase(request.get("context").asInstanceOf[String])
    val courseIdOpt = ViewerRequestKeys.courseId(request)
    val batchIdOpt = ViewerRequestKeys.batchId(request)
    val contentIds: util.List[String] = request.get("contentId") match {
      case l: util.List[_] => l.asScala.map(_.asInstanceOf[String]).asJava
      case s: String if StringUtils.isNotBlank(s) => util.Arrays.asList(s)
      case _ => null
    }
    val hasContent = contentIds != null && !contentIds.isEmpty
    // read must be anchored: a specific content (organic) or a collection (whole-collection read). contextall drops the context only.
    if (!hasContent && courseIdOpt.isEmpty)
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, "contentId or collectionId is required")
    // individual content (no collection): PK collapses to the single contentId for collection+context (scenario 1)
    val singleContent = if (courseIdOpt.isEmpty && hasContent && contentIds.size == 1) contentIds.get(0) else null
    val collectionId = courseIdOpt.getOrElse(singleContent)
    val contextId = batchIdOpt.orElse(courseIdOpt).getOrElse(singleContent)
    val filters = new util.HashMap[String, AnyRef]()
    filters.put("userid", userId)
    if (collectionId != null) filters.put("collectionid", collectionId)
    if (!allContexts && contextId != null) filters.put("contextid", contextId)
    if (hasContent) filters.put("contentid", contentIds)
    val response = cassandraOperation.getRecords(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE,
      filters.asInstanceOf[util.Map[String, AnyRef]], null, ctx)
    val rows = response.getResult.getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]])
      .asInstanceOf[util.List[util.Map[String, AnyRef]]]
    // per-item operational fields (viewCount/last*Time/completionPercentage) are retained so content/state can reconstruct its ucc row
    val contents = new util.ArrayList[util.Map[String, AnyRef]]()
    rows.asScala.foreach(r => contents.add(toContentItem(r)))
    val out = new Response()
    out.put(JsonKey.USER_ID, userId)
    out.put("collectionId", courseIdOpt.orNull)
    out.put("contextId", batchIdOpt.orNull)
    out.put("type", if (allContexts) "contextall" else "content")
    out.put("contents", contents)
    sender().tell(out, self)
  }

  // allow-list: view.read emits only the fields we need; any other column in the row (old_*, userId, lastUpdatedTime, …)
  // is ignored by default. Accepts both key casings (prod camelCase from CassandraUtil / raw lowercase).
  private def toContentItem(r: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val item = new util.HashMap[String, AnyRef]()
    putIf(item, "contentId", firstVal(r, "contentId", "contentid"))
    putIf(item, "collectionId", firstVal(r, "collectionid", "collectionId"))
    putIf(item, "contextId", firstVal(r, "contextid", "contextId"))
    putIf(item, "status", firstVal(r, "status"))
    putIf(item, "progress", firstVal(r, "progress"))
    putIf(item, "viewCount", firstVal(r, "viewCount", "viewcount"))
    putIf(item, "completedCount", firstVal(r, "completedCount", "completedcount"))
    putIf(item, "completionPercentage", firstVal(r, "completionPercentage", "completionpercentage"))
    putIf(item, "lastAccessTime", firstVal(r, "lastAccessTime", "last_access_time"))
    putIf(item, "lastCompletedTime", firstVal(r, "lastCompletedTime", "last_completed_time"))
    putIf(item, "dateTime", firstVal(r, "dateTime", "datetime"))
    putIf(item, "addedBy", firstVal(r, "addedBy", "addedby"))
    Option(firstVal(r, "progressdetails", "progressDetails")).foreach(v => item.put("progressDetails", parseProgressDetails(v)))
    item
  }

  private def firstVal(r: util.Map[String, AnyRef], keys: String*): AnyRef = keys.iterator.map(r.get).find(_ != null).orNull
  private def putIf(m: util.Map[String, AnyRef], k: String, v: AnyRef): Unit = if (v != null) m.put(k, v)

  private def parseProgressDetails(v: AnyRef): AnyRef = v match {
    case s: String if StringUtils.isNotBlank(s) =>
      try mapper.readValue(s, classOf[util.Map[String, AnyRef]]) catch { case _: Throwable => s }
    case other => other
  }

  private def viewStart(request: Request): Unit = {
    val ctx = request.getRequestContext
    val key = viewKey(request)
    val existing = readRow(key, ctx)
    if (existing == null) {
      val row = new util.HashMap[String, AnyRef](key)
      row.put("status", Integer.valueOf(1))
      row.put("last_access_time", mergeTime(null, "last_access_time", request, JsonKey.LAST_ACCESS_TIME))
      row.put("last_updated_time", ProjectUtil.getTimeStamp)
      applyClientFields(request, null, row)
      cassandraOperation.upsertRecord(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE, row.asInstanceOf[util.Map[String, AnyRef]], ctx)
      logger.info(ctx, s"view: start inserted | user=${key.get("userid")} course=${key.get("collectionid")} batch=${key.get("contextid")} content=${key.get("contentid")}")
    } else {
      // row exists: merge client progress/viewcount/lastAccessTime monotonically (max / later-of)
      val row = new util.HashMap[String, AnyRef](key)
      if (applyClientFields(request, existing, row)) {
        row.put("last_updated_time", ProjectUtil.getTimeStamp)
        cassandraOperation.upsertRecord(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE, row.asInstanceOf[util.Map[String, AnyRef]], ctx)
      }
      logger.info(ctx, s"view: start noop(exists) | user=${key.get("userid")} content=${key.get("contentid")}")
    }
    touchEnrolmentAccess(key, 1, ctx)
    sender().tell(ackResponse(key.get("contentid").asInstanceOf[String], "Progress started"), self)
  }

  private def viewUpdate(request: Request): Unit = {
    val ctx = request.getRequestContext
    val key = viewKey(request)
    val existing = readRow(key, ctx)
    if (existing != null && statusOf(existing) < 2) {
      val row = new util.HashMap[String, AnyRef](key)
      // status is monotonic; an update never downgrades and never completes (that's viewEnd)
      row.put("status", Integer.valueOf(math.max(1, statusOf(existing))))
      Option(request.get("progressDetails")).foreach(pd => row.put("progressdetails", mapper.writeValueAsString(pd)))
      row.put("last_access_time", ProjectUtil.getTimeStamp)
      row.put("last_updated_time", ProjectUtil.getTimeStamp)
      cassandraOperation.upsertRecord(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE, row.asInstanceOf[util.Map[String, AnyRef]], ctx)
      logger.info(ctx, s"view: update merged | user=${key.get("userid")} content=${key.get("contentid")}")
    } else logger.info(ctx, s"view: update skip(absent-or-completed) | user=${key.get("userid")} content=${key.get("contentid")}")
    touchEnrolmentAccess(key, math.max(1, if (existing != null) statusOf(existing) else 1), ctx)
    sender().tell(ackResponse(key.get("contentid").asInstanceOf[String], "Progress Updated"), self)
  }

  private def viewEnd(request: Request): Unit = {
    val ctx = request.getRequestContext
    val key = viewKey(request)
    val existing = readRow(key, ctx)
    val row = new util.HashMap[String, AnyRef](key)
    row.put("status", Integer.valueOf(2))
    row.put("progress", Integer.valueOf(100)) // viewEnd always completes
    Option(request.get("progressDetails")).foreach(pd => row.put("progressdetails", mapper.writeValueAsString(pd)))
    row.put("last_completed_time", mergeCompletedTime(existing, request))
    row.put("last_access_time", mergeTime(existing, "last_access_time", request, JsonKey.LAST_ACCESS_TIME))
    Option(request.get(JsonKey.VIEW_COUNT)).foreach(v =>
      row.put("viewcount", Integer.valueOf(math.max(intOf(v.asInstanceOf[AnyRef]), if (existing != null) intOf(existing.get("viewcount")) else 0))))
    row.put("last_updated_time", ProjectUtil.getTimeStamp)
    cassandraOperation.upsertRecord(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE, row.asInstanceOf[util.Map[String, AnyRef]], ctx)
    logger.info(ctx, s"view: end completed | user=${key.get("userid")} course=${key.get("collectionid")} batch=${key.get("contextid")} content=${key.get("contentid")}")
    touchEnrolmentAccess(key, 2, ctx)
    triggerAggregation(request, ctx)
    sender().tell(ackResponse(key.get("contentid").asInstanceOf[String], "Progress ended"), self)
  }

  // fire-and-forget tell to the aggregator; rollup runs async, the response does not wait
  private def triggerAggregation(request: Request, ctx: RequestContext): Unit = {
    val key = viewKey(request)
    val aggRequest = new Request()
    aggRequest.setOperation("aggregate")
    aggRequest.setRequestContext(ctx)
    aggRequest.put(JsonKey.USER_ID, key.get("userid"))
    aggRequest.put("courseId", key.get("collectionid"))
    aggRequest.put("batchId", key.get("contextid"))
    logger.info(ctx, s"view: rollup triggered | user=${key.get("userid")} course=${key.get("collectionid")} batch=${key.get("contextid")}")
    viewerAggregatorActor.tell(aggRequest, ActorRef.noSender)
  }

  // ucc primary key; missing keys cascade: courseid <- contentId, batchid <- courseId <- contentId (design scenarios 1-3)
  private def viewKey(request: Request): util.HashMap[String, AnyRef] = {
    // explicit userId (internal delegation) else requestedFor/requestedBy (from token on direct API calls)
    val userId = Option(request.get(JsonKey.USER_ID).asInstanceOf[String]).filter(StringUtils.isNotBlank)
      .orElse(Option(request.get(JsonKey.REQUESTED_FOR).asInstanceOf[String]).filter(StringUtils.isNotBlank))
      .getOrElse(request.get(JsonKey.REQUESTED_BY).asInstanceOf[String])
    val contentId = ViewerRequestKeys.contentId(request)
    val courseId = ViewerRequestKeys.courseId(request).getOrElse(contentId)
    val batchId = ViewerRequestKeys.batchId(request).orElse(ViewerRequestKeys.courseId(request)).getOrElse(contentId)
    val key = new util.HashMap[String, AnyRef]()
    key.put("userid", userId)
    key.put("collectionid", courseId)
    key.put("contextid", batchId)
    key.put("contentid", contentId)
    key
  }

  private def readRow(key: util.HashMap[String, AnyRef], ctx: RequestContext): util.Map[String, AnyRef] = {
    val filters = new util.HashMap[String, AnyRef](key)
    val response = cassandraOperation.getRecords(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE,
      filters.asInstanceOf[util.Map[String, AnyRef]], null, ctx)
    val rows = response.getResult
      .getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]])
      .asInstanceOf[util.List[util.Map[String, AnyRef]]]
    if (CollectionUtils.isNotEmpty(rows)) rows.get(0) else null
  }

  private def statusOf(row: util.Map[String, AnyRef]): Int =
    Option(row.get("status")).map(_.asInstanceOf[Number].intValue()).getOrElse(0)

  private def intOf(v: AnyRef): Int = Option(v).map(_.asInstanceOf[Number].intValue()).getOrElse(0)

  /** epoch millis from a Number, a numeric String, or an ISO-8601 String (assessmentTs is a String in the contract). */
  private def parseMillis(v: AnyRef): Option[Long] = v match {
    case n: Number => Some(n.longValue())
    case s: String if StringUtils.isNotBlank(s) =>
      try Some(s.toLong) catch { case _: Throwable => try Some(java.time.Instant.parse(s).toEpochMilli) catch { case _: Throwable => None } }
    case _ => None
  }

  /** Merge client progress/viewcount/lastAccessTime into `row` monotonically (max / later-of); true if any set. */
  private def applyClientFields(request: Request, existing: util.Map[String, AnyRef], row: util.HashMap[String, AnyRef]): Boolean = {
    var changed = false
    Option(request.get("progress")).foreach { p =>
      val incoming = intOf(p.asInstanceOf[AnyRef])
      val current = if (existing != null) intOf(existing.get("progress")) else 0
      row.put("progress", Integer.valueOf(math.max(incoming, current)))
      changed = true
    }
    Option(request.get(JsonKey.VIEW_COUNT)).foreach { v =>
      val incoming = intOf(v.asInstanceOf[AnyRef])
      val current = if (existing != null) intOf(existing.get("viewcount")) else 0
      row.put("viewcount", Integer.valueOf(math.max(incoming, current)))
      changed = true
    }
    Option(request.get(JsonKey.LAST_ACCESS_TIME)).foreach { _ =>
      row.put("last_access_time", mergeTime(existing, "last_access_time", request, JsonKey.LAST_ACCESS_TIME))
      changed = true
    }
    changed
  }

  /** last_completed_time for viewEnd: later of existing vs client-forwarded lastCompletedTime, else now. */
  private def mergeCompletedTime(existing: util.Map[String, AnyRef], request: Request): java.util.Date =
    mergeTime(existing, "last_completed_time", request, JsonKey.LAST_COMPLETED_TIME)

  private def mergeTime(existing: util.Map[String, AnyRef], existingCol: String, request: Request, requestKey: String): java.util.Date = {
    val existingTime: java.util.Date = if (existing != null) existing.get(existingCol).asInstanceOf[java.util.Date] else null
    val inputTime: java.util.Date = Option(request.get(requestKey)).flatMap {
      case d: java.util.Date => Some(d)
      case n: Number => Some(new java.util.Date(n.longValue()))
      case s: String if StringUtils.isNotBlank(s) =>
        try Some(new java.util.Date(s.toLong)) catch { case _: Throwable => try Some(java.util.Date.from(java.time.Instant.parse(s))) catch { case _: Throwable => None } }
      case _ => None
    }.orNull
    if (existingTime == null && inputTime == null) ProjectUtil.getTimeStamp
    else if (existingTime == null) inputTime
    else if (inputTime == null) existingTime
    else if (inputTime.after(existingTime)) inputTime else existingTime
  }

  // for tests
  def setCassandraOperation(ops: org.sunbird.cassandra.CassandraOperation): ViewConsumptionActor = {
    cassandraOperation = ops
    this
  }
}
