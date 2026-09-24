package org.sunbird.viewer.actor

import com.google.gson.Gson
import org.apache.commons.collections4.CollectionUtils
import org.sunbird.activity.domain.{ActorObject, ContentStatus, EventContext, EventData, EventObject, TelemetryEvent, UserContentConsumption, UserEnrolmentAgg}
import org.sunbird.activity.util.{ActivityAggregateUtil, CertificateUtil, HierarchyRelationsUtil}
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.common.ProjectUtil
import org.sunbird.exception.ProjectCommonException
import org.sunbird.response.ResponseCode
import org.sunbird.enrolments.BaseEnrolmentActor
import org.sunbird.helper.ServiceFactory
import org.sunbird.kafka.KafkaClient
import org.sunbird.keys.JsonKey
import org.sunbird.learner.util.Util
import org.sunbird.request.{Request, RequestContext}

import java.util
import scala.collection.JavaConverters._

// Recursive rollup (reuses ActivityAggregateUtil); optionality is per-user (user_enrolments.optional_nodes), recomputed from DB each call -> idempotent.
class ViewerAggregatorActor extends BaseEnrolmentActor {

  private var cassandraOperation: CassandraOperation = ServiceFactory.getInstance
  private var hierarchyRelationsUtil: HierarchyRelationsUtil = HierarchyRelationsUtil(cassandraOperation)
  private val activityAggUtil = new ActivityAggregateUtil()
  private var certificateUtil: CertificateUtil = CertificateUtil()
  private val gson = new Gson()
  private def auditEventTopic = Option(ProjectUtil.getConfigValue("kafka_topics_audit_event")).getOrElse("dev.telemetry.raw")

  private val enrolmentDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB)
  private val activityAggDBInfo = Util.dbInfoMap.get(JsonKey.GROUP_ACTIVITY_DB)
  private val consumptionDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB)
  private val CONSUMPTION_TABLE = "user_content_consumption"

  override def onReceive(request: Request): Unit = {
    request.getOperation match {
      case "aggregate" =>
        try {
          aggregate(request)
          sender().tell(successResponse(), self)
        } catch {
          case ex: Exception =>
            logger.error(request.getRequestContext, s"ViewerAggregatorActor.aggregate failed: ${ex.getMessage}", ex)
            ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR, ex.getMessage)
        }
      case _           => onReceiveUnsupportedOperation(request.getOperation)
    }
  }

  private def aggregate(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    // accept courseId (else legacy courseId) / batchId (else legacy batchId)
    val courseId = ViewerRequestKeys.courseId(request).orNull
    val batchId = ViewerRequestKeys.batchId(request).orNull
    if (userId == null || courseId == null) {
      logger.warn(ctx, s"ViewerAggregatorActor: missing userId/courseId, skipping", null)
      return
    }

    logger.info(ctx, s"viewer.rollup: start | user=$userId course=$courseId batch=$batchId")

    // 1. Read this user's consumption for the (root) collection+context from viewer ucc, build status map
    val rows = readConsumption(userId, courseId, batchId, ctx)
    if (CollectionUtils.isEmpty(rows)) {
      logger.info(ctx, s"viewer.rollup: no-consumption skip | user=$userId course=$courseId")
      return
    }
    val contentStatusMap: Map[String, ContentStatus] = activityAggUtil.getContentStatusFromContents(rows)
    val uc = UserContentConsumption(userId, batchId, courseId, contentStatusMap)

    // 2. Per-learner optional COURSES from user_enrolments.optional_nodes (LP policy; course-level).
    val perLearnerOptionalCourses: List[String] = readOptionalNodes(userId, courseId, batchId, ctx)

    // 3. Root leaves + the tree's nodes (via ancestors) — needed before computing effectiveOptional.
    val leafNodes = hierarchyRelationsUtil.getLeafNodes(courseId, courseId, ctx)
    if (leafNodes.isEmpty) {
      logger.warn(ctx, s"ViewerAggregatorActor: no leafNodes for courseId=$courseId; is hierarchy_relations published?", null)
      return
    }
    val ancestors: Map[String, List[String]] = uc.contents.map { case (contentId, content) =>
      (contentId, hierarchyRelationsUtil.getAncestors(courseId, content.contentId, ctx))
    }.toMap
    val childCollections = ancestors.values.flatten.filter(_ != courseId).toList.distinct

    // effectiveOptional leaves = author-marked hierarchy optionalnodes ∪ leaves of per-learner optional courses
    val treeNodes = courseId :: childCollections
    val hierarchyOptionalLeaves = treeNodes.flatMap(n => hierarchyRelationsUtil.getOptionalNodes(courseId, n, ctx)).distinct
    val optionalCourseLeaves = perLearnerOptionalCourses.flatMap(c => hierarchyRelationsUtil.getLeafNodes(courseId, c, ctx)).distinct
    val effectiveOptional: List[String] = (hierarchyOptionalLeaves ++ optionalCourseLeaves).distinct

    // 4. Aggregates: root + every ancestor node; required per node = its leafNodes − effectiveOptional.
    val courseAgg = activityAggUtil.computeCourseActivityAgg(uc, leafNodes, effectiveOptional, ctx)
    val collectionsWithLeafNodes: Map[String, List[String]] = childCollections.map { col =>
      (col, hierarchyRelationsUtil.getLeafNodes(courseId, col, ctx).diff(effectiveOptional))
    }.toMap
    val moduleAggs = activityAggUtil.computeModuleActivityAgg(uc, courseId, ancestors, collectionsWithLeafNodes, ctx)

    val allAggs: List[UserEnrolmentAgg] = courseAgg.toList ++ moduleAggs

    // 5. Write user_activity_agg (frozen content_status + agg) for root + every node
    writeActivityAggregates(allAggs, ctx)
    logger.info(ctx, s"viewer.rollup: nodes rolled-up n=${allAggs.size} | user=$userId course=$courseId batch=$batchId")

    // 6. Per-node progress: nodeId -> (completedCount, requiredLeaves) for root + every trackable ancestor.
    val nodeProgress = scala.collection.mutable.LinkedHashMap[String, (Int, List[String])]()
    courseAgg.foreach(a => nodeProgress(courseId) = (completedCountOf(a), leafNodes.diff(effectiveOptional)))
    moduleAggs.foreach(a => nodeProgress(a.activityAgg.activity_id) =
      (completedCountOf(a), collectionsWithLeafNodes.getOrElse(a.activityAgg.activity_id, Nil)))

    // 7. Update user_enrolments status for every enrolled node in this tree (keyed off existing rows; root included)
    writeAllNodeEnrolments(userId, courseId, batchId, nodeProgress.toMap, contentStatusMap, ctx)
  }

  private def completedCountOf(a: UserEnrolmentAgg): Int =
    a.activityAgg.aggregates.getOrElse("completedCount", 0.0).toInt

  private def writeActivityAggregates(aggs: List[UserEnrolmentAgg], ctx: RequestContext): Unit = {
    val aggQueries = aggs.map(a => activityAggUtil.createActivityAggUpdateMap(a.activityAgg)).asJava
    if (!aggQueries.isEmpty)
      cassandraOperation.batchUpdateWithPutAll(activityAggDBInfo.getKeySpace, activityAggDBInfo.getTableName, aggQueries, ctx)
  }

  // update every node's enrolment row in this tree; matches on courseid ∈ tree AND this LP's batchid (standalone enrolments untouched)
  private def writeAllNodeEnrolments(userId: String, rootId: String, batchId: String,
                                     nodeProgress: Map[String, (Int, List[String])],
                                     contentStatusMap: Map[String, ContentStatus], ctx: RequestContext): Unit = {
    val filters = new util.HashMap[String, AnyRef]() {{ put("userid", userId) }}
    val enrolRows = cassandraOperation.getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName,
      filters.asInstanceOf[util.Map[String, AnyRef]], null, ctx)
      .getResult.getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]])
      .asInstanceOf[util.List[util.Map[String, AnyRef]]]
    enrolRows.asScala.foreach { row =>
      // createResponse maps DB columns to camelCase (courseid->courseId, batchid->batchId)
      val nodeId = Option(row.get("courseId")).map(_.toString).orNull
      val nodeCtx = Option(row.get("batchId")).map(_.toString).orNull
      // This LP only (root=batchId, child=batchId:childId); a standalone enrolment's batchid differs (§4).
      val expectedCtx = if (nodeId == rootId) batchId else batchId + ":" + nodeId
      nodeProgress.get(nodeId).filter(_ => nodeCtx == expectedCtx).foreach { case (completedCount, requiredLeaves) =>
        val required = requiredLeaves.size
        val status = activityAggUtil.getCompletionStatus(completedCount, required)
        val pct = activityAggUtil.getCompletionPercentage(completedCount, required)
        val currentStatus = Option(row.get("status")).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
        val nodeContentStatus: Map[String, AnyRef] =
          requiredLeaves.flatMap(l => contentStatusMap.get(l).map(cs => l -> Integer.valueOf(cs.status).asInstanceOf[AnyRef])).toMap
        // updateRecordV2 replaces the whole contentstatus column, so merge into the existing map to avoid clobbering unseen leaves
        val mergedContentStatus = ViewerAggregatorActor.mergeContentStatus(row.get("contentStatus"), nodeContentStatus)
        val selectMap = new util.HashMap[String, AnyRef]() {{
          put("userid", userId); put("courseid", nodeId); put("batchid", nodeCtx)
        }}
        val updateMap = new util.HashMap[String, AnyRef]() {{
          put("progress", Integer.valueOf(completedCount))
          put("status", Integer.valueOf(status))
          put("completionpercentage", Integer.valueOf(pct))
          put("contentstatus", mergedContentStatus)
          if (status == 2 && currentStatus != 2) put("completedon", new java.util.Date())
        }}
        cassandraOperation.updateRecordV2(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, selectMap, updateMap, true, ctx)
        if (status == 2 && currentStatus != 2) {
          logger.info(ctx, s"viewer.rollup: node completed | user=$userId course=$nodeId batch=$nodeCtx")
          publishCompletionEvents(userId, nodeId, nodeCtx, ctx)
        }
      }
    }
  }

  // on completion: same events as the legacy activity-aggregator — issue-certificate + enrol-complete audit (best-effort; a kafka hiccup must not fail the rollup)
  private def publishCompletionEvents(userId: String, courseId: String, batchId: String, ctx: RequestContext): Unit = {
    try {
      certificateUtil.publishCertificateIssueEvent(userId, courseId, batchId, ctx)
      val auditEvent = TelemetryEvent(
        actor = ActorObject(id = userId, `type` = "User"),
        edata = EventData(props = Array("status", "completedon"), `type` = "enrol-complete"),
        context = EventContext(cdata = Array(
          Map("type" -> "CourseBatch", "id" -> batchId).asJava,
          Map("type" -> "Course", "id" -> courseId).asJava
        )),
        `object` = EventObject(id = userId, `type` = "User", rollup = Map("l1" -> courseId).asJava)
      )
      KafkaClient.send(gson.toJson(auditEvent), auditEventTopic)
    } catch {
      case ex: Exception => logger.error(ctx, s"viewer.rollup: completion event publish failed | user=$userId course=$courseId batch=$batchId", ex)
    }
  }

  // clustering-prefix slice on ucc PK (userid, collectionid, contextid); contextId omitted only when absent (no-context read)
  private def readConsumption(userId: String, courseId: String, batchId: String, ctx: RequestContext): util.List[util.Map[String, AnyRef]] = {
    val filters = new util.HashMap[String, AnyRef]() {{
      put("userid", userId)
      put("collectionid", courseId)
      if (batchId != null) put("contextid", batchId)
    }}
    val response = cassandraOperation.getRecords(consumptionDBInfo.getKeySpace, CONSUMPTION_TABLE,
      filters.asInstanceOf[util.Map[String, AnyRef]], null, ctx)
    response.getResult.getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]])
      .asInstanceOf[util.List[util.Map[String, AnyRef]]]
  }

  /** Per-user optional_nodes from user_enrolments (empty for strict policy). */
  private def readOptionalNodes(userId: String, courseId: String, batchId: String, ctx: RequestContext): List[String] = {
    val filters = new util.HashMap[String, AnyRef]() {{
      put("userid", userId)
      put("courseid", courseId)
      if (batchId != null) put("batchid", batchId)
    }}
    val response = cassandraOperation.getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName,
      filters.asInstanceOf[util.Map[String, AnyRef]], null, ctx)
    val rows = response.getResult.getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]])
      .asInstanceOf[util.List[util.Map[String, AnyRef]]]
    if (CollectionUtils.isNotEmpty(rows)) {
      Option(rows.get(0).get("optional_nodes"))
        .map(_.asInstanceOf[util.Collection[String]].asScala.toList)
        .getOrElse(List())
    } else List()
  }

  // for tests
  def configure(ops: CassandraOperation, hru: HierarchyRelationsUtil): ViewerAggregatorActor = {
    cassandraOperation = ops; hierarchyRelationsUtil = hru; this
  }
}

object ViewerAggregatorActor {
  // merge fresh per-leaf statuses into the existing contentstatus map (updateRecordV2 overwrites the whole column); fresh wins on conflict
  private[actor] def mergeContentStatus(existing: AnyRef, fresh: Map[String, AnyRef]): java.util.Map[String, AnyRef] = {
    val merged = new java.util.HashMap[String, AnyRef]()
    Option(existing).foreach(m => merged.putAll(m.asInstanceOf[java.util.Map[String, AnyRef]]))
    fresh.foreach { case (k, v) => merged.put(k, v) }
    merged
  }
}
