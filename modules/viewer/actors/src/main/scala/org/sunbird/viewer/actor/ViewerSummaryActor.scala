package org.sunbird.viewer.actor

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.sunbird.common.ProjectUtil
import org.sunbird.enrolments.BaseEnrolmentActor
import org.sunbird.exception.ProjectCommonException
import org.sunbird.helper.ServiceFactory
import org.sunbird.keys.JsonKey
import org.sunbird.learner.util.{ContentSearchUtil, Util}
import org.sunbird.utils.CloudStorageUtil
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.{Response, ResponseCode}

import java.util
import scala.collection.JavaConverters._

// Summary APIs over user_enrolments (never writes consumption). Collection/assessment enrichment is fail-safe (empty on error).
class ViewerSummaryActor extends BaseEnrolmentActor {

  private var cassandraOperation = ServiceFactory.getInstance
  private val enrolmentDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB)
  private val assessmentDBInfo = Util.dbInfoMap.get(JsonKey.ASSESSMENT_AGGREGATOR_DB)
  private val consumptionDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB)
  private val activityAggDBInfo = Util.dbInfoMap.get(JsonKey.GROUP_ACTIVITY_DB)
  private val mapper = new ObjectMapper

  override def onReceive(request: Request): Unit = {
    request.getOperation match {
      case "summaryRead"     => summaryRead(request)
      case "summaryList"     => summaryList(request)
      case "summaryDownload" => summaryDownload(request)
      case "summaryDelete"   => summaryDelete(request)
      case _                 => onReceiveUnsupportedOperation(request.getOperation)
    }
  }

  // summary.read returns a single enriched object (not a list); collectionId + contextId are mandatory (identify one enrolment)
  private def summaryRead(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val courseId = ViewerRequestKeys.courseId(request).orNull
    val batchId = ViewerRequestKeys.batchId(request).orNull
    if (StringUtils.isBlank(courseId) || StringUtils.isBlank(batchId))
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, "collectionId and contextId are required")

    val filters = new util.HashMap[String, AnyRef]()
    filters.put("userid", userId)
    filters.put("courseid", courseId)
    filters.put("batchid", batchId)
    val enrolments = getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, filters, ctx)
    logger.info(ctx, s"summary: read | user=$userId course=$courseId rows=${enrolments.size}")
    val response = new Response
    if (!enrolments.isEmpty) {
      val collections = collectionsMap(collectionIdsOf(enrolments), request)
      val assess = assessmentMap(userId, collectionIdsOf(enrolments), ctx)
      toSummary(enrolments.get(0), collections, assess, ctx).asScala.foreach { case (k, v) => response.put(k, v) }
    }
    sender().tell(response, self)
  }

  private def summaryList(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = Option(request.get(JsonKey.USER_ID).asInstanceOf[String])
      .getOrElse(request.get("userId").asInstanceOf[String])
    val filters = new util.HashMap[String, AnyRef]()
    filters.put("userid", userId)
    val enrolments = getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, filters, ctx)
    logger.info(ctx, s"summary: list | user=$userId rows=${enrolments.size}")
    val collections = collectionsMap(collectionIdsOf(enrolments), request)
    val assess = assessmentMap(userId, collectionIdsOf(enrolments), ctx)
    val summaries = new util.ArrayList[util.Map[String, AnyRef]]()
    enrolments.asScala.foreach(r => summaries.add(toSummary(r, collections, assess, ctx)))
    val response = new Response
    response.put("summary", summaries)
    sender().tell(response, self)
  }

  // summary.download uploads a csv/json file (default json) and returns { url }
  private def summaryDownload(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = Option(request.get(JsonKey.USER_ID).asInstanceOf[String])
      .getOrElse(request.get("userId").asInstanceOf[String])
    val format = Option(request.get("format").asInstanceOf[String]).map(_.toLowerCase).getOrElse("json")
    val filters = new util.HashMap[String, AnyRef]() {{ put("userid", userId) }}
    val enrolments = getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, filters, ctx)
    val (ext, content) =
      if (format == "csv") ("csv", toCsv(enrolments))
      else {
        val collections = collectionsMap(collectionIdsOf(enrolments), request)
        val assess = assessmentMap(userId, collectionIdsOf(enrolments), ctx)
        val summaries = new util.ArrayList[util.Map[String, AnyRef]]()
        enrolments.asScala.foreach(r => summaries.add(toSummary(r, collections, assess, ctx)))
        ("json", mapper.writeValueAsString(summaries))
      }
    val response = new Response
    response.put("url", uploadSummary(userId, ext, content))
    logger.info(ctx, s"summary: download | user=$userId format=$format rows=${enrolments.size}")
    sender().tell(response, self)
  }

  private def toSummary(row: util.Map[String, AnyRef], collections: util.Map[String, util.Map[String, AnyRef]],
                        assess: Map[(String, String), util.Map[String, AnyRef]], ctx: RequestContext): util.Map[String, AnyRef] = {
    val userId = str(row.get("userId"))
    val collectionId = str(firstNonNull(row.get("courseId"), row.get("courseid")))
    val contextId = str(firstNonNull(row.get("batchId"), row.get("batchid")))
    val s = new util.LinkedHashMap[String, AnyRef]()
    s.put("userId", userId)
    s.put("collectionId", collectionId)
    s.put("contextId", contextId)
    s.put("enrolledDate", firstNonNull(row.get("enrolledDate"), row.get("enrolleddate"), row.get("oldEnrolledDate")))
    s.put("active", row.getOrDefault("active", java.lang.Boolean.TRUE))
    s.put("contentStatus", firstNonNull(row.get("contentStatus"), row.get("contentstatus")))
    s.put("assessmentStatus", assess.getOrElse((collectionId, contextId), new util.HashMap[String, AnyRef]()))
    s.put("collection", collectionBlock(collections.get(collectionId), collectionId))
    s.put("issuedCertificates", firstNonNull(row.get("issuedCertificates"), row.get("issued_certificates"), new util.ArrayList[AnyRef]()))
    s.put("completedOn", firstNonNull(row.get("completedOn"), row.get("completedon")))
    s.put("progress", row.getOrDefault("progress", Integer.valueOf(0)))
    s.put("status", row.getOrDefault("status", Integer.valueOf(0)))
    s
  }

  private def collectionIdsOf(enrolments: util.List[util.Map[String, AnyRef]]): util.List[String] = {
    val ids = new util.ArrayList[String]()
    enrolments.asScala.foreach(r => Option(str(firstNonNull(r.get("courseId"), r.get("courseid"))))
      .filter(StringUtils.isNotBlank).foreach(ids.add))
    ids
  }

  // one batch content search for all collectionIds -> { collectionId -> content } (fail-safe empty on error)
  private def collectionsMap(collectionIds: util.List[String], request: Request): util.Map[String, util.Map[String, AnyRef]] = {
    val out = new util.HashMap[String, util.Map[String, AnyRef]]()
    if (collectionIds.isEmpty) return out
    try {
      val filters = new util.HashMap[String, AnyRef]() {{ put(JsonKey.IDENTIFIER, collectionIds); put(JsonKey.STATUS, "Live") }}
      val body = mapper.writeValueAsString(new util.HashMap[String, AnyRef]() {{
        put(JsonKey.REQUEST, new util.HashMap[String, AnyRef]() {{ put(JsonKey.FILTERS, filters); put(JsonKey.LIMIT, Integer.valueOf(collectionIds.size)) }})
      }})
      val headers = Option(request.get(JsonKey.HEADER).asInstanceOf[util.Map[String, String]]).getOrElse(new util.HashMap[String, String]())
      val result = ContentSearchUtil.searchContentSync(request.getRequestContext, "", body, headers)
      val contents = result.getOrDefault(JsonKey.CONTENTS, new util.ArrayList[util.Map[String, AnyRef]]())
        .asInstanceOf[util.List[util.Map[String, AnyRef]]]
      contents.asScala.foreach(c => Option(c.get(JsonKey.IDENTIFIER)).foreach(id => out.put(id.asInstanceOf[String], c)))
    } catch { case e: Throwable => logger.info(request.getRequestContext, s"summary: collection search skipped: ${e.getMessage}") }
    out
  }

  private def collectionBlock(content: util.Map[String, AnyRef], collectionId: String): util.Map[String, AnyRef] = {
    val out = new util.HashMap[String, AnyRef]()
    if (content == null) return out
    out.put("identifier", content.getOrDefault(JsonKey.IDENTIFIER, collectionId))
    out.put("name", content.get(JsonKey.NAME))
    out.put("logo", content.get(JsonKey.APP_ICON))
    out.put("leafNodesCount", content.get(JsonKey.LEAF_NODE_COUNT))
    out.put("description", content.get(JsonKey.DESCRIPTION))
    out
  }

  // one IN query for all collections -> { (collectionId, contextId) -> { contentId -> {score, max_score} } } (best attempt per content, fail-safe)
  private def assessmentMap(userId: String, collectionIds: util.List[String], ctx: RequestContext): Map[(String, String), util.Map[String, AnyRef]] = {
    if (collectionIds.isEmpty) return Map.empty
    try {
      val filters = new util.HashMap[String, AnyRef]()
      filters.put("user_id", userId)
      filters.put("collection_id", collectionIds) // List -> IN across the (user_id, collection_id) partitions
      val rows = getRecords(assessmentDBInfo.getKeySpace, assessmentDBInfo.getTableName, filters, ctx)
      rows.asScala.groupBy(r => (str(firstNonNull(r.get("collection_id"), r.get("collectionId"))),
                                 str(firstNonNull(r.get("context_id"), r.get("contextId"))))).map { case (key, attempts) =>
        val perContent = new util.HashMap[String, AnyRef]()
        attempts.groupBy(a => str(firstNonNull(a.get("content_id"), a.get("contentId")))).foreach {
          case (cid, atts) if cid != null =>
            val best = atts.maxBy(a => num(firstNonNull(a.get("totalScore"), a.get("total_score"))))
            val m = new util.HashMap[String, AnyRef]()
            m.put("score", firstNonNull(best.get("totalScore"), best.get("total_score")))
            m.put("max_score", firstNonNull(best.get("totalMaxScore"), best.get("total_max_score")))
            perContent.put(cid, m)
          case _ =>
        }
        key -> perContent
      }
    } catch { case e: Throwable => logger.info(ctx, s"summary: assessment batch skipped: ${e.getMessage}"); Map.empty }
  }

  private def uploadSummary(userId: String, ext: String, content: String): String = {
    val storageType = ProjectUtil.getConfigValue("sunbird_cloud_service_provider")
    val container = Option(ProjectUtil.getConfigValue("viewer_summary_cloud_storage_container")).filter(StringUtils.isNotBlank)
      .getOrElse(ProjectUtil.getConfigValue("sunbird_content_cloud_storage_container"))
    val prefix = Option(ProjectUtil.getConfigValue("viewer_summary_upload_path")).getOrElse("").trim.stripSuffix("/")
    val objectKey = (if (StringUtils.isNotBlank(prefix)) prefix + "/" else "") + userId + "_viewer_summary." + ext
    val tmp = java.io.File.createTempFile(userId + "_viewer_summary", "." + ext)
    try {
      val w = new java.io.PrintWriter(tmp, "UTF-8")
      try w.write(content) finally w.close()
      CloudStorageUtil.upload(storageType, container, objectKey, tmp.getAbsolutePath)
    } finally tmp.delete()
  }

  // (csv header, result-row key)
  private val csvCols = List(
    ("courseid", "courseId"), ("batchid", "batchId"), ("status", "status"),
    ("completionpercentage", "completionPercentage"), ("progress", "progress"),
    ("completedon", "completedOn"), ("enrolleddate", "enrolledDate"),
    ("lastcontentaccesstime", "lastContentAccessTime"), ("certificatescount", "issuedCertificates"))
  // RFC-4180: quote any field containing a comma, quote, or line break; escape embedded quotes as "".
  private def csvField(v: String): String =
    if (v.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r')) "\"" + v.replace("\"", "\"\"") + "\""
    else v
  private def csvCell(r: util.Map[String, AnyRef], key: String): String = key match {
    case "issuedCertificates" => Option(r.get(key)).collect { case c: util.Collection[_] => c.size.toString }.getOrElse("0")
    case _ => Option(r.get(key)).map(_.toString).getOrElse("")
  }
  private def toCsv(rows: util.List[util.Map[String, AnyRef]]): String = {
    val sb = new StringBuilder(csvCols.map(_._1).mkString(",")).append("\n")
    rows.asScala.foreach { r =>
      sb.append(csvCols.map { case (_, key) => csvField(csvCell(r, key)) }.mkString(",")).append("\n")
    }
    sb.toString
  }

  // atomic purge: remove the learner's footprint for (user, collection, context) across enrolment, consumption,
  // assessments, and the rollup. Deleting only the enrolment would leave consumption that silently resurrects on re-enrol.
  private def summaryDelete(request: Request): Unit = {
    val ctx = request.getRequestContext
    val userId = Option(request.get(JsonKey.USER_ID).asInstanceOf[String])
      .getOrElse(request.get("userId").asInstanceOf[String])
    val courseId = ViewerRequestKeys.courseId(request).orNull
    val batchId = ViewerRequestKeys.batchId(request).orNull

    val purged = new util.ArrayList[util.Map[String, AnyRef]]()
    if (StringUtils.isBlank(courseId)) {
      val rows = getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName,
        new util.HashMap[String, AnyRef]() {{ put("userid", userId) }}, ctx)
      rows.asScala.foreach { r =>
        val col = strOrNull(firstNonNull(r.get("courseId"), r.get("courseid")))
        val bat = strOrNull(firstNonNull(r.get("batchId"), r.get("batchid")))
        purge(userId, col, bat, ctx); purged.add(purgedEntry(col, bat))
      }
    } else {
      purge(userId, courseId, batchId, ctx); purged.add(purgedEntry(courseId, batchId))
    }
    logger.info(ctx, s"summary: purge | user=$userId enrolments=${purged.size}")
    val response = new Response
    response.put(userId, "Enrolment Deleted Succesfully")
    response.put("purged", purged)
    sender().tell(response, self)
  }

  // delete the (user, collection, context) footprint across all four tables; each delete is fail-safe so one failure doesn't strand the rest
  private def purge(userId: String, collectionId: String, contextId: String, ctx: RequestContext): Unit = {
    deleteBy(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, key("userid" -> userId, "courseid" -> collectionId, "batchid" -> contextId), ctx)
    deleteBy(consumptionDBInfo.getKeySpace, consumptionDBInfo.getTableName, key("userid" -> userId, "collectionid" -> collectionId, "contextid" -> contextId), ctx)
    deleteBy(assessmentDBInfo.getKeySpace, assessmentDBInfo.getTableName, key("user_id" -> userId, "collection_id" -> collectionId, "context_id" -> contextId), ctx)
    // user_activity_agg keys context_id as "cb:"+batchId for a course batch; skip only the nested-module aggregate rows, which self-heal on the next rollup
    if (StringUtils.isNotBlank(contextId))
      deleteBy(activityAggDBInfo.getKeySpace, activityAggDBInfo.getTableName,
        key("activity_type" -> "Course", "activity_id" -> collectionId, "user_id" -> userId, "context_id" -> ("cb:" + contextId)), ctx)
  }

  private def deleteBy(keyspace: String, table: String, k: util.Map[String, String], ctx: RequestContext): Unit =
    try cassandraOperation.deleteRecord(keyspace, table, k, ctx)
    catch { case e: Throwable => logger.info(ctx, s"summary: purge skip $table: ${e.getMessage}") }

  private def key(kvs: (String, String)*): util.Map[String, String] = {
    val m = new util.HashMap[String, String]()
    kvs.foreach { case (k, v) => if (StringUtils.isNotBlank(v)) m.put(k, v) }
    m
  }

  private def purgedEntry(collectionId: String, contextId: String): util.Map[String, AnyRef] =
    new util.HashMap[String, AnyRef]() {{ put("collectionId", collectionId); put("contextId", contextId) }}

  private def getRecords(keyspace: String, table: String, filters: util.HashMap[String, AnyRef], ctx: RequestContext): util.List[util.Map[String, AnyRef]] = {
    val response = cassandraOperation.getRecords(keyspace, table, filters.asInstanceOf[util.Map[String, AnyRef]], null, ctx)
    response.getResult.getOrDefault(JsonKey.RESPONSE, new util.ArrayList[util.Map[String, AnyRef]])
      .asInstanceOf[util.List[util.Map[String, AnyRef]]]
  }

  private def str(v: AnyRef): String = if (v == null) null else v.toString
  private def strOrNull(v: AnyRef): String = if (v == null) null else v.asInstanceOf[String]
  private def firstNonNull(vs: AnyRef*): AnyRef = vs.find(_ != null).orNull
  private def num(v: AnyRef): Double = v match {
    case n: Number => n.doubleValue()
    case s: String if StringUtils.isNotBlank(s) => try s.toDouble catch { case _: Throwable => 0.0 }
    case _ => 0.0
  }

  // for tests
  def setCassandraOperation(ops: org.sunbird.cassandra.CassandraOperation): ViewerSummaryActor = {
    cassandraOperation = ops
    this
  }
}
