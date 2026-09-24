package org.sunbird.activity.actor

import com.google.gson.Gson
import org.apache.pekko.actor.Props
import org.apache.commons.lang3.StringUtils
import org.sunbird.activity.domain.{CollectionProgress, ContentStatus, TelemetryEvent, UserContentConsumption, UserEnrolmentAgg}
import org.sunbird.activity.util.{ActivityAggregateUtil, CertificateUtil, ContentSearchUtil, DeDupUtil, HierarchyRelationsUtil}
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.exception.ProjectCommonException
import org.sunbird.keys.JsonKey
import org.sunbird.logging.LoggerUtil
import org.sunbird.common.ProjectUtil
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.ResponseCode
import org.sunbird.enrolments.BaseEnrolmentActor
import org.sunbird.helper.ServiceFactory
import org.sunbird.http.HttpClientUtil
import org.sunbird.kafka.KafkaClient
import org.sunbird.learner.util.Util

import java.util
import scala.collection.JavaConverters._

class ActivityAggregatorActor extends BaseEnrolmentActor {

  private var cassandraOperation: CassandraOperation = ActivityAggregatorActor.sharedCassandraOperation
  private var hierarchyRelationsUtil: HierarchyRelationsUtil = ActivityAggregatorActor.sharedHierarchyRelationsUtil
  private var certificateUtil: CertificateUtil = ActivityAggregatorActor.sharedCertificateUtil
  private var contentSearchUtil: ContentSearchUtil = ActivityAggregatorActor.sharedContentSearchUtil
  private var deDupUtil: DeDupUtil = ActivityAggregatorActor.sharedDeDupUtil

  private def gson = ActivityAggregatorActor.sharedGson
  private def enrolmentDBInfo = ActivityAggregatorActor.sharedEnrolmentDBInfo
  private def activityAggDBInfo = ActivityAggregatorActor.sharedActivityAggDBInfo
  private def consumptionDBInfo = ActivityAggregatorActor.sharedConsumptionDBInfo
  private def auditEventTopic = ActivityAggregatorActor.sharedAuditEventTopic
  private def moduleAggEnabled = ActivityAggregatorActor.sharedModuleAggEnabled
  private def filterCompletedEnrolments = ActivityAggregatorActor.sharedFilterCompletedEnrolments
  private def dedupEnabled = ActivityAggregatorActor.sharedDedupEnabled

  private val activityAggUtil = new ActivityAggregateUtil()

  override def onReceive(request: Request): Unit = {
    request.getOperation match {
      case "updateActivityAggregates" => updateActivityAggregates(request)
      case _ => onReceiveUnsupportedOperation(request.getOperation)
    }
  }

  private def updateActivityAggregates(request: Request): Unit = {
    val requestContext = request.getRequestContext
    val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
    val batchId = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
    val courseId = request.get(JsonKey.COURSE_ID).asInstanceOf[String]
    val contentsRaw = request.get(JsonKey.CONTENTS)
    val contents = if (contentsRaw != null) contentsRaw.asInstanceOf[util.List[util.Map[String, AnyRef]]] else null

    try {
      if (isViewerEnabled) {
        val token = Option(request.getContext.get(JsonKey.X_AUTH_TOKEN)).map(_.asInstanceOf[String]).orNull
        delegateToViewer(userId, courseId, batchId, contents, token, requestContext)
      } else {
        processActivityAggregates(userId, batchId, courseId, contents, requestContext)
      }
      sender().tell(successResponse(), self)
    } catch {
      case ex: Exception =>
        logger.error(requestContext, s"ActivityAggregatorActor failed for userId: $userId, courseId: $courseId", ex)
        ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR, ex.getMessage)
    }
  }

  private def isViewerEnabled: Boolean =
    java.lang.Boolean.parseBoolean(ProjectUtil.getConfigValue("viewer_enabled"))

  private def viewerBaseUrl: String =
    Option(ProjectUtil.getConfigValue("viewer_service_base_url")).filter(StringUtils.isNotBlank)
      .getOrElse("http://viewer-service:9000")

  private def isMonolith: Boolean = !"distributed".equalsIgnoreCase(ProjectUtil.getConfigValue("deployment_mode"))

  // /v1/activity/agg adapter -> viewer: contents map to viewStart/viewEnd, none -> aggregate; transport monolith tell vs distributed HTTP
  private def delegateToViewer(userId: String, courseId: String, batchId: String,
                               contents: util.List[util.Map[String, AnyRef]], token: String,
                               ctx: RequestContext): Unit = {
    val headers = new util.HashMap[String, String]() {{
      put("Content-Type", "application/json")
      Option(token).filter(StringUtils.isNotBlank).foreach(t => put("x-authenticated-user-token", t))
    }}
    def dispatch(actorName: String, api: String, operation: String, body: util.Map[String, AnyRef]): Unit = {
      if (isMonolith) {
        val req = new Request(); req.setRequestContext(ctx); req.setOperation(operation); req.setRequest(body)
        context.actorSelection("/user/" + actorName).tell(req, org.apache.pekko.actor.ActorRef.noSender)
      } else {
        val envelope = new util.HashMap[String, AnyRef]() {{ put(JsonKey.REQUEST, body) }}
        HttpClientUtil.post(viewerBaseUrl + api, gson.toJson(envelope), headers, ctx)
      }
    }
    if (contents != null && !contents.isEmpty) {
      contents.asScala.foreach { c =>
        val status = Option(c.get(JsonKey.STATUS)).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
        val (op, api) = if (status >= 2) ("viewEnd", "/v1/view/end") else ("viewStart", "/v1/view/start")
        dispatch("view-consumption-actor", api, op, new util.HashMap[String, AnyRef]() {{
          put("contentId", c.get(JsonKey.CONTENT_ID))
          put("courseId", courseId)
          put("batchId", batchId)
          put(JsonKey.USER_ID, userId)
          Option(c.get("progressdetails")).orElse(Option(c.get("progressDetails"))).foreach(pd => put("progressDetails", pd))
          Option(c.get(JsonKey.PROGRESS)).foreach(p => put(JsonKey.PROGRESS, p))
          Option(c.get(JsonKey.VIEW_COUNT)).foreach(v => put(JsonKey.VIEW_COUNT, v))
          Option(c.get(JsonKey.LAST_ACCESS_TIME)).foreach(v => put(JsonKey.LAST_ACCESS_TIME, v))
          Option(c.get(JsonKey.LAST_COMPLETED_TIME)).foreach(v => put(JsonKey.LAST_COMPLETED_TIME, v))
        }})
      }
    } else {
      dispatch("viewer-aggregator-actor", "/v1/view/agg", "aggregate", new util.HashMap[String, AnyRef]() {{
        put("courseId", courseId)
        put("batchId", batchId)
        put(JsonKey.USER_ID, userId)
      }})
    }
  }

  private def processActivityAggregates(
                                         userId: String,
                                         batchId: String,
                                         courseId: String,
                                         contents: util.List[util.Map[String, AnyRef]],
                                         requestContext: RequestContext
                                       ): Unit = {
    logger.info(requestContext, s"ActivityAggregatorActor: START processActivityAggregates - userId: $userId, courseId: $courseId, batchId: $batchId")
    
    var dbUserConsumption: UserContentConsumption = null
    
    val uniqueContents = if (contents != null && !contents.isEmpty) {
      val filteredContents = filterValidContents(contents)
      if (filteredContents.isEmpty) {
        logger.info(requestContext, s"No valid contents to process for userId: $userId, courseId: $courseId")
        return
      }
      
      val unique = deduplicateContents(filteredContents, userId, batchId, courseId, requestContext)
      if (unique.isEmpty) {
        logger.info(requestContext, s"No unique contents after deduplication for userId: $userId, courseId: $courseId")
        return
      }
      unique
    } else if (contents == null) {
      logger.info(requestContext, s"Contents key missing, fetching from DB (Force Sync) for userId: $userId, courseId: $courseId")
      dbUserConsumption = getContentStatusFromDB(userId, courseId, batchId, requestContext)
      if (dbUserConsumption.contents.isEmpty) {
        logger.info(requestContext, s"No existing consumption in DB to sync for userId: $userId, courseId: $courseId")
        return
      }
      dbUserConsumption.contents.values.map(c => {
         val m = new java.util.HashMap[String, AnyRef]()
         m.put(JsonKey.CONTENT_ID, c.contentId)
         m.put(JsonKey.STATUS, c.status.asInstanceOf[AnyRef])
         m.put(JsonKey.LAST_ACCESS_TIME, c.lastAccessTime)
         m.put(JsonKey.COMPLETED_COUNT, c.completedCount.asInstanceOf[AnyRef])
         m.put(JsonKey.VIEW_COUNT, c.viewCount.asInstanceOf[AnyRef])
         m.put(JsonKey.PROGRESS, c.progress.asInstanceOf[AnyRef])
         if (c.lastUpdatedTime != null) m.put(JsonKey.LAST_UPDATED_TIME, c.lastUpdatedTime)
         if (c.lastCompletedTime != null) m.put(JsonKey.LAST_COMPLETED_TIME, c.lastCompletedTime)
         m
      }).toList
    } else {
      logger.info(requestContext, s"Received empty contents list. No processing required.")
      return
    }
    
    val contentStatusMap = activityAggUtil.getContentStatusFromContents(uniqueContents.asJava)
    val inputUserConsumption = UserContentConsumption(userId, batchId, courseId, contentStatusMap)
    
    if (dbUserConsumption == null) {
      dbUserConsumption = getContentStatusFromDB(userId, courseId, batchId, requestContext)
    }
    val finalUserConsumption = activityAggUtil.mergeConsumptionData(inputUserConsumption, dbUserConsumption)
    
    updateContentConsumption(finalUserConsumption, requestContext)
    
    // Fetch leaf nodes and optional nodes from Yugabyte hierarchy_relations table
    val leafNodes = hierarchyRelationsUtil.getLeafNodes(courseId, courseId, requestContext)
    val optionalNodes = hierarchyRelationsUtil.getOptionalNodes(courseId, courseId, requestContext)
    
    if (leafNodes.nonEmpty) {
      val courseAggregations = computeCourseAggregations(finalUserConsumption, courseId, leafNodes, optionalNodes, requestContext)
      updateActivityAggregates(courseAggregations, requestContext)
      
      val collectionProgressList = courseAggregations.filter(agg => agg.collectionProgress.nonEmpty).map(agg => agg.collectionProgress.get)
      val latestRead = activityAggUtil.getLatestReadDetails(userId, batchId, courseId, uniqueContents)
      updateCollectionProgress(collectionProgressList, leafNodes, optionalNodes, latestRead, requestContext)
      
      if (dedupEnabled) {
        uniqueContents.foreach { content =>
          val contentId = Option(content.get(JsonKey.CONTENT_ID)).getOrElse(content.get("contentid")).asInstanceOf[String]
          val status = content.get("status").asInstanceOf[Number].intValue()
          val checksum = deDupUtil.getMessageId(courseId, batchId, userId, contentId, status)
          deDupUtil.storeChecksum(checksum, requestContext)
        }
      }
      
      publishContentAuditEvents(finalUserConsumption, requestContext)
      logger.info(requestContext, s"ActivityAggregatorActor: Successfully processed all aggregates for userId: $userId, courseId: $courseId")
    } else {
      handleMissingLeafNodes(courseId, requestContext)
    }
  }

  private def filterValidContents(contents: util.List[util.Map[String, AnyRef]]): List[util.Map[String, AnyRef]] = {
    val filtered = contents.asScala.filter(c => {
      val contentId = Option(c.get(JsonKey.CONTENT_ID)).getOrElse(c.get("contentid")).asInstanceOf[String]
      val status = c.getOrDefault("status", 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
      StringUtils.isNotBlank(contentId) && status > 0
    }).toList
    if (contents.size() != filtered.size) {
      logger.info(s"filterValidContents: Filtered ${contents.size()} -> ${filtered.size} valid contents")
    }
    filtered
  }

  private def deduplicateContents(
                                   contents: List[util.Map[String, AnyRef]],
                                   userId: String,
                                   batchId: String,
                                   courseId: String,
                                   requestContext: RequestContext
                                 ): List[util.Map[String, AnyRef]] = {
    if (!dedupEnabled) {
      logger.info(requestContext, s"deduplicateContents: Deduplication disabled, returning all ${contents.size} contents")
      return contents
    }
    
    logger.info(requestContext, s"deduplicateContents: Checking ${contents.size} contents for duplicates")
    val unique = contents.filter(content => {
      val contentId = Option(content.get(JsonKey.CONTENT_ID)).getOrElse(content.get("contentid")).asInstanceOf[String]
      val status = content.get("status").asInstanceOf[Number].intValue()
      val checksum = deDupUtil.getMessageId(courseId, batchId, userId, contentId, status)
      deDupUtil.isUniqueEvent(checksum, requestContext)
    })
    logger.info(requestContext, s"deduplicateContents: Found ${unique.size} unique contents out of ${contents.size}")
    unique
  }

  private def updateContentConsumption(userConsumption: UserContentConsumption, requestContext: RequestContext): Unit = {
    logger.info(requestContext, s"updateContentConsumption: Creating batch update queries for ${userConsumption.contents.size} records")
    val queries: java.util.List[java.util.Map[String, java.util.Map[String, Object]]] = 
      new java.util.ArrayList[java.util.Map[String, java.util.Map[String, Object]]]()
    
    userConsumption.contents.foreach { case (contentId, content) =>
      val updateMaps = activityAggUtil.createContentConsumptionUpdateMap(
        userConsumption.userId,
        userConsumption.courseId,
        userConsumption.batchId,
        content
      )
      val queryMap: java.util.Map[String, java.util.Map[String, Object]] = new util.HashMap[String, java.util.Map[String, Object]]()
      queryMap.put(JsonKey.PRIMARY_KEY, updateMaps._1.asInstanceOf[java.util.Map[String, Object]])
      queryMap.put(JsonKey.NON_PRIMARY_KEY, updateMaps._2.asInstanceOf[java.util.Map[String, Object]])
      queries.add(queryMap)
    }
    
    if (!queries.isEmpty) {
      logger.info(requestContext, s"updateContentConsumption: Executing batch update with ${queries.size()} queries")
      cassandraOperation.batchUpdate(consumptionDBInfo.getKeySpace, "user_content_consumption", queries, requestContext)
      logger.info(requestContext, s"updateContentConsumption: Batch update completed successfully")
    } else {
      logger.warn(requestContext, s"updateContentConsumption: No queries to execute", null)
    }
  }

  private def computeCourseAggregations(
                                         userConsumption: UserContentConsumption,
                                         courseId: String,
                                         leafNodes: List[String],
                                         optionalNodes: List[String],
                                         requestContext: RequestContext
                                       ): List[UserEnrolmentAgg] = {
    logger.info(requestContext, s"computeCourseAggregations: Computing course-level aggregation for courseId: $courseId")
    val courseAggOpt = activityAggUtil.computeCourseActivityAgg(userConsumption, leafNodes, optionalNodes, requestContext)
    val courseAggs = if (courseAggOpt.nonEmpty) {
      logger.info(requestContext, s"computeCourseAggregations: Course aggregation computed successfully")
      List(courseAggOpt.get)
    } else {
      logger.warn(requestContext, s"computeCourseAggregations: No course aggregation computed", null)
      List()
    }
    
    if (moduleAggEnabled) {
      logger.info(requestContext, s"computeCourseAggregations: Module aggregation enabled, computing module-level aggregations")
      val ancestors = userConsumption.contents.map { case (contentId, content) =>
        val ancestorList = hierarchyRelationsUtil.getAncestors(courseId, content.contentId, requestContext)
        logger.info(requestContext, s"computeCourseAggregations: contentId: $contentId has ${ancestorList.size} ancestors")
        (contentId, ancestorList)
      }.toMap
      
      val childCollections = ancestors.values.flatten.filter(a => a != courseId).toList.distinct
      logger.info(requestContext, s"computeCourseAggregations: Found ${childCollections.size} child collections: ${childCollections.mkString(", ")}")
      
      val collectionsWithLeafNodes = childCollections.map(collectionId => {
        val collectionLeafNodes = hierarchyRelationsUtil.getRequiredLeafNodes(courseId, collectionId, requestContext)
        logger.info(requestContext, s"computeCourseAggregations: collectionId: $collectionId has ${collectionLeafNodes.size} required leaf nodes")
        (collectionId, collectionLeafNodes)
      }).toMap

      val moduleAggs = activityAggUtil.computeModuleActivityAgg(userConsumption, courseId, ancestors, collectionsWithLeafNodes, requestContext)
      logger.info(requestContext, s"computeCourseAggregations: Computed ${moduleAggs.size} module aggregations")
      courseAggs ++ moduleAggs
    } else {
      logger.info(requestContext, s"computeCourseAggregations: Module aggregation disabled, returning only course aggregation")
      courseAggs
    }
  }

  private def updateActivityAggregates(courseAggregations: List[UserEnrolmentAgg], requestContext: RequestContext): Unit = {
    val aggQueries = courseAggregations.map { agg =>
      activityAggUtil.createActivityAggUpdateMap(agg.activityAgg)
    }.asJava
    
    if (!aggQueries.isEmpty) {
      cassandraOperation.batchUpdateWithPutAll(activityAggDBInfo.getKeySpace, activityAggDBInfo.getTableName, aggQueries, requestContext)
    }
  }

  private def updateCollectionProgress(
                                        collectionProgressList: List[CollectionProgress],
                                        leafNodes: List[String],
                                        optionalNodes: List[String],
                                        latestRead: util.Map[String, AnyRef],
                                        requestContext: RequestContext
                                      ): Unit = {
    collectionProgressList.foreach { progress =>
      val shouldUpdateProgress = if (filterCompletedEnrolments) {
        val enrolmentStatus = getEnrolmentStatus(progress.userId, progress.courseId, progress.batchId, requestContext)
        enrolmentStatus != 2
      } else true

      if (shouldUpdateProgress) {
        val updatedLeafNodes = leafNodes.diff(optionalNodes)
        val completionStatus = activityAggUtil.getCompletionStatus(progress.progress, updatedLeafNodes.size)
        val completionPercentage = activityAggUtil.getCompletionPercentage(progress.progress, updatedLeafNodes.size)

        val progressUpdateMap = activityAggUtil.createProgressUpdateMap(
          progress.userId, progress.courseId, progress.batchId,
          progress.progress,
          completionStatus,
          completionPercentage,
          progress.completedOn,
          progress.contentStatus,
          latestRead
        )
        cassandraOperation.updateRecordV2(enrolmentDBInfo.getKeySpace, 
          enrolmentDBInfo.getTableName, progressUpdateMap._1, progressUpdateMap._2, true, requestContext)

        if (progress.completed) {
          logger.info(requestContext, s"updateCollectionProgress: Course completed for userId: ${progress.userId}, Publishing certificate event")
          certificateUtil.publishCertificateIssueEvent(progress.userId, progress.courseId, progress.batchId, requestContext)
          publishEnrolmentCompleteAuditEvent(progress, requestContext)
        }
      }
    }
  }


  private def publishContentAuditEvents(userConsumption: UserContentConsumption, requestContext: RequestContext): Unit = {
    val auditEvents = activityAggUtil.generateContentAuditEvents(userConsumption)
    if (auditEvents.nonEmpty) {
      logger.info(requestContext, s"publishContentAuditEvents: Publishing ${auditEvents.size} audit events to topic: $auditEventTopic")
      auditEvents.foreach { event =>
        publishAuditEvent(event, requestContext)
      }
    }
  }

  protected def publishEnrolmentCompleteAuditEvent(progress: CollectionProgress, requestContext: RequestContext): Unit = {
    import org.sunbird.activity.domain._
    
    val auditEvent = TelemetryEvent(
      actor = ActorObject(id = progress.userId, `type` = "User"),
      edata = EventData(props = Array("status", "completedon"), `type` = "enrol-complete"),
      context = EventContext(cdata = Array(
        Map("type" -> "CourseBatch", "id" -> progress.batchId).asJava,
        Map("type" -> "Course", "id" -> progress.courseId).asJava
      )),
      `object` = EventObject(id = progress.userId, `type` = "User", rollup = Map("l1" -> progress.courseId).asJava)
    )
    publishAuditEvent(auditEvent, requestContext)
  }

  protected def publishAuditEvent(event: TelemetryEvent, requestContext: RequestContext): Unit = {
    try {
      val eventJson = gson.toJson(event)
      logger.info(requestContext, s"publishAuditEvent: Publishing event to Kafka - topic: $auditEventTopic")
      KafkaClient.send(eventJson, auditEventTopic)
      logger.info(requestContext, s"publishAuditEvent: Event published successfully")
    } catch {
      case ex: Exception =>
        logger.error(requestContext, s"publishAuditEvent: Failed to publish audit event to Kafka topic $auditEventTopic: ${ex.getMessage}", ex)
    }
  }

  private def handleMissingLeafNodes(courseId: String, requestContext: RequestContext): Unit = {
    val collectionStatus = contentSearchUtil.getCollectionStatus(courseId, requestContext)
    
    if (StringUtils.equalsIgnoreCase(collectionStatus, "Retired")) {
      logger.warn(requestContext, s"Contents consumed from retired collection: $courseId", null)
    } else {
      val errorMsg = s"Leaf nodes not available for published collection: $courseId (status: $collectionStatus)"
      logger.error(requestContext, errorMsg, null)
      throw new Exception(errorMsg)
    }
  }

  private def getEnrolmentStatus(userId: String, courseId: String, batchId: String, requestContext: RequestContext): Int = {
    logger.info(requestContext, s"getEnrolmentStatus: Querying user_enrolments for userId: $userId, courseId: $courseId, batchId: $batchId")
    val selectMap = new util.HashMap[String, AnyRef]() {{
      put("userid", userId)
      put("courseid", courseId)
      put("batchid", batchId)
    }}
    
    val response = cassandraOperation.getRecordsByProperties(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, selectMap, requestContext)
    
    if (response != null && response.getResult != null) {
      val result = response.getResult.get(JsonKey.RESPONSE).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      if (!result.isEmpty) {
        val enrolment = result.get(0)
        val status = enrolment.getOrDefault("status", 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
        logger.info(requestContext, s"getEnrolmentStatus: Found enrolment with status: $status")
        return status
      } else {
        logger.info(requestContext, s"getEnrolmentStatus: No enrolment found, returning status 0")
      }
    } else {
      logger.warn(requestContext, s"getEnrolmentStatus: Null response from Cassandra", null)
    }
    
    0
  }

  private def getContentStatusFromDB(userId: String, courseId: String, batchId: String, requestContext: RequestContext): UserContentConsumption = {
    logger.info(requestContext, s"getContentStatusFromDB: Querying user_content_consumption for userId: $userId, courseId: $courseId, batchId: $batchId")
    val response = cassandraOperation.getRecordsByProperties(consumptionDBInfo.getKeySpace, "user_content_consumption",
      new util.HashMap[String, AnyRef]() {{
        put("userid", userId)
        put("collectionid", courseId)
        put("contextid", batchId)
      }}, requestContext)
    
    if (response != null && response.getResult != null) {
      val result = response.getResult.get(JsonKey.RESPONSE).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      logger.info(requestContext, s"getContentStatusFromDB: Found ${result.size()} records in DB")
      
      if (!result.isEmpty) {
        
        val consumptionMap = result.asScala.flatMap(row => {
          val contentId = Option(row.get("contentid"))
            .orElse(Option(row.get("contentId")))
            .orElse(Option(row.get("content_id")))
            .map(_.asInstanceOf[String]).getOrElse("")
            
          if (StringUtils.isNotBlank(contentId)) {
            val status = Option(row.get("status")).orElse(Option(row.get("Status"))).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
            val viewCount = Option(row.get("viewcount")).orElse(Option(row.get("viewCount"))).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
            val completedCount = Option(row.get("completedcount")).orElse(Option(row.get("completedCount"))).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
            val progress = Option(row.get("progress")).orElse(Option(row.get("Progress"))).map(_.asInstanceOf[Number].intValue()).getOrElse(0)
            
            val lastAccessTime = activityAggUtil.parseDate(Option(row.get("lastaccesstime")).getOrElse(row.get("lastAccessTime")))
            val lastCompletedTime = activityAggUtil.parseDate(Option(row.get("lastcompletedtime")).getOrElse(row.get("lastCompletedTime")))
            val lastUpdatedTime = activityAggUtil.parseDate(Option(row.get("lastupdatedtime")).getOrElse(row.get("lastUpdatedTime")))
            
            Some(contentId -> ContentStatus(contentId, status, completedCount, viewCount, progress, lastAccessTime, lastCompletedTime, lastUpdatedTime, fromInput = false))
          } else {
            logger.warn(requestContext, s"getContentStatusFromDB: Skipping row with missing contentId. Keys: ${row.keySet()}", null)
            None
          }
        }).toMap
        
        logger.info(requestContext, s"getContentStatusFromDB: Returning ${consumptionMap.size} content status records")
        return UserContentConsumption(userId, batchId, courseId, consumptionMap)
      }
    }
    
    logger.info(requestContext, s"getContentStatusFromDB: No existing consumption found, returning empty")
    UserContentConsumption(userId, batchId, courseId, Map.empty)
  }
}

object ActivityAggregatorActor {

  // Shared singletons — initialized once per JVM, reused across all actor instances in the pool
  lazy val sharedCassandraOperation: CassandraOperation = ServiceFactory.getInstance
  lazy val sharedHierarchyRelationsUtil: HierarchyRelationsUtil = HierarchyRelationsUtil(sharedCassandraOperation)
  lazy val sharedCertificateUtil: CertificateUtil = CertificateUtil()
  lazy val sharedContentSearchUtil: ContentSearchUtil = ContentSearchUtil()
  lazy val sharedDeDupUtil: DeDupUtil = DeDupUtil()
  lazy val sharedGson: Gson = new Gson()

  // DB info — lazy guards against access before Util.dbInfoMap is populated at startup
  lazy val sharedEnrolmentDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB)
  lazy val sharedActivityAggDBInfo = Util.dbInfoMap.get(JsonKey.GROUP_ACTIVITY_DB)
  lazy val sharedConsumptionDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB)

  // Config values — read once per JVM, not per actor instance
  lazy val sharedAuditEventTopic: String = ProjectUtil.getConfigValue("kafka_topics_audit_event") match {
    case value if value != null => value
    case _ => "dev.telemetry.raw"
  }
  lazy val sharedModuleAggEnabled: Boolean = ProjectUtil.getConfigValue("enable_module_aggregation") match {
    case value if value != null => value.toBoolean
    case _ => true
  }
  lazy val sharedFilterCompletedEnrolments: Boolean = ProjectUtil.getConfigValue("filter_processed_enrolments") match {
    case value if value != null => value.toBoolean
    case _ => true
  }
  lazy val sharedDedupEnabled: Boolean = ProjectUtil.getConfigValue("activity_input_dedup_enabled") match {
    case value if value != null => value.toBoolean
    case _ => false
  }

  def props(): Props = Props(new ActivityAggregatorActor())
}