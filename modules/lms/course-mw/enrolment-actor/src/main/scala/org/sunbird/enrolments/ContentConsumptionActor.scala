package org.sunbird.enrolments

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections4.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.common.CassandraUtil
import org.sunbird.exception.ProjectCommonException
import org.sunbird.response.Response
import org.sunbird.keys.JsonKey
import org.sunbird.telemetry.dto.TelemetryEnvKey
import org.sunbird.request.{Request, RequestContext}
import org.sunbird.response.ResponseCode
import org.sunbird.utils.JsonUtil
import org.sunbird.common.ProjectUtil
import org.sunbird.helper.ServiceFactory
import org.sunbird.http.HttpClientUtil
import org.sunbird.kafka.{InstructionEventGenerator, KafkaClient}
import org.sunbird.learner.constants.{CourseJsonKey, InstructionEvent}
import org.sunbird.learner.util.{CourseBatchUtil, Util}

import com.datastax.driver.core.{UDTValue, UserType}
import java.util
import java.util.{Date, TimeZone, UUID}
import javax.inject.{Inject, Named}
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._

case class InternalContentConsumption(courseId: String, batchId: String, contentId: String) {
  def validConsumption() = StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(batchId) && StringUtils.isNotBlank(contentId)
}

class ContentConsumptionActor @Inject() (
    @Named("activity-aggregator-actor") activityAggregatorActor: ActorRef,
    @Named("assessment-aggregator-actor") assessmentAggregatorActor: ActorRef
) extends BaseEnrolmentActor {
    private val mapper = new ObjectMapper
    private var cassandraOperation = ServiceFactory.getInstance
    private var pushTokafkaEnabled: Boolean = true //TODO: to be removed once all are in scala
    private val consumptionDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_CONTENT_DB)
    private val assessmentAggregatorDBInfo = Util.dbInfoMap.get(JsonKey.ASSESSMENT_AGGREGATOR_DB)
    private val enrolmentDBInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB)
    val dateFormatter = ProjectUtil.getDateFormatter
    val jsonFields = Set[String]("progressdetails")
    private lazy val questionUDTType = cassandraOperation.getUDTType(assessmentAggregatorDBInfo.getKeySpace, "question")

    override def onReceive(request: Request): Unit = {
        Util.initializeContext(request, TelemetryEnvKey.BATCH, this.getClass.getName)

        dateFormatter.setTimeZone(
            TimeZone.getTimeZone(ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TIMEZONE)))

        request.getOperation match {
            case "updateConsumption" => updateConsumption(request)
            case "getConsumption" => getConsumption(request)
            case "syncAssessmentData" => handleSyncAssessmentData(request)
            case _ => onReceiveUnsupportedOperation(request.getOperation)
        }
    }

    def handleSyncAssessmentData(request: Request): Unit = {
        val requestContext = request.getRequestContext
        val requestBy = request.get(JsonKey.REQUESTED_BY).asInstanceOf[String]
        val requestedFor = request.get(JsonKey.REQUESTED_FOR).asInstanceOf[String]
        val assessmentEvents = request.getRequest.getOrDefault(JsonKey.ASSESSMENT_EVENTS, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        if (CollectionUtils.isNotEmpty(assessmentEvents)) {
            val userAssessments = updateAssessEventUserid(assessmentEvents.asScala.toList, requestBy, requestedFor)
            userAssessments.values.flatten.foreach(assessment => {
                syncAssessmentData(assessment, requestContext)
            })
        }
        sender().tell(successResponse(), self)
    }

    def updateConsumption(request: Request): Unit = {
        val requestBy = request.get(JsonKey.REQUESTED_BY).asInstanceOf[String]
        val requestedFor = request.get(JsonKey.REQUESTED_FOR).asInstanceOf[String]
        val assessmentEvents = request.getRequest.getOrDefault(JsonKey.ASSESSMENT_EVENTS, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val contentList = request.getRequest.getOrDefault(JsonKey.CONTENTS, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        if(CollectionUtils.isEmpty(contentList) && CollectionUtils.isEmpty(assessmentEvents)) {
            processEnrolmentSync(request, requestBy, requestedFor)
        } else {
            val requestContext = request.getRequestContext
            val assessmentEvents = request.getRequest.getOrDefault(JsonKey.ASSESSMENT_EVENTS, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
            val contentList = request.getRequest.getOrDefault(JsonKey.CONTENTS, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
            // viewer.enabled -> Viewer Service owns BOTH: contents -> /v1/view/start|end, assessments -> /v1/assessment/submit
            // (submit scores + marks status=2 + runs the rollup itself, so assessments are NOT merged into contents here).
            // viewer disabled -> legacy: merge assessments as completed contents, then processContents + processAssessments.
            val contentConsumptionResponse =
              if (isViewerEnabled) delegateContentsToViewer(contentList, request, requestBy, requestedFor)
              else {
                val finalContentList = if(CollectionUtils.isNotEmpty(assessmentEvents)) {
                  logger.info(requestContext, "Assessment Consumption events exist: " + assessmentEvents.size())
                  val assessmentConsumptions = assessmentEvents.map(e => {
                    InternalContentConsumption(e.get("courseId").asInstanceOf[String], e.get("batchId").asInstanceOf[String], e.get("contentId").asInstanceOf[String])
                  }).filter(cc => cc.validConsumption()).map(cc => {
                    val consumption: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
                    consumption.put("courseId", cc.courseId)
                    consumption.put("batchId", cc.batchId)
                    consumption.put("contentId", cc.contentId)
                    consumption.put("status", 2.asInstanceOf[AnyRef])
                    consumption
                  })
                  if (CollectionUtils.isNotEmpty(contentList)) (contentList ++ assessmentConsumptions).asJava else assessmentConsumptions.asJava
                } else contentList
                logger.info(requestContext, "Final content-consumption data: " + finalContentList)
                processContents(finalContentList, requestContext, requestBy, requestedFor)
              }
            val assessmentResponse =
              if (isViewerEnabled) delegateAssessmentsToViewer(assessmentEvents, request, requestBy, requestedFor)
              else processAssessments(assessmentEvents, requestContext, requestBy, requestedFor)
            val finalResponse = assessmentResponse.getOrElse(new Response())
            finalResponse.putAll(contentConsumptionResponse.getOrElse(new Response()).getResult)
            sender().tell(finalResponse, self)
        }
    }
    def updateAssessEventUserid(data: List[java.util.Map[String, AnyRef]], requestedBy: String, requestedFor: String): Map[String, List[util.Map[String, AnyRef]]] = {
        val primaryUserId = if(StringUtils.isNotBlank(requestedFor)) requestedFor else requestedBy
        val updatedData: java.util.List[java.util.Map[String, AnyRef]] = data.map(assess => {
            assess.put(JsonKey.USER_ID, primaryUserId)
            val assessEvents = assess.getOrDefault(JsonKey.ASSESSMENT_EVENTS_KEY, new java.util.ArrayList[java.util.Map[String, AnyRef]])
              .asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
            val assessEventData :java.util.List[java.util.Map[String, AnyRef]]= assessEvents.map(event=> {
                val actorEvent = event.getOrDefault(JsonKey.ASSESSMENT_ACTOR,
                    new java.util.HashMap[String,AnyRef]).asInstanceOf[java.util.Map[String,AnyRef]]
                if(!actorEvent.isEmpty) {
                    actorEvent.put("id",primaryUserId)
                    event.put("actor",actorEvent)
                }
                event
            }).toList
            assess.put("events",assessEventData)
            assess
        })
        updatedData.toList.groupBy(d => d.get(JsonKey.USER_ID).asInstanceOf[String])
    }

    def processAssessments(assessmentEvents: java.util.List[java.util.Map[String, AnyRef]], requestContext: RequestContext, requestedBy: String, requestedFor: String): Option[Response] = {
        if(CollectionUtils.isNotEmpty(assessmentEvents)) {
            val batchAssessmentList: Map[String, List[java.util.Map[String, AnyRef]]] = assessmentEvents.filter(event => StringUtils.isNotBlank(event.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String])).toList.groupBy(event => event.get(JsonKey.BATCH_ID).asInstanceOf[String])
            val batchIds = batchAssessmentList.keySet.toList.asJava
            val batches:Map[String, List[java.util.Map[String, AnyRef]]] = getBatches(requestContext ,new java.util.ArrayList[String](batchIds), null).toList.groupBy(batch => batch.get(JsonKey.BATCH_ID).asInstanceOf[String])
            val invalidBatchIds = batchAssessmentList.keySet.diff(batches.keySet).toList.asJava
            val validBatches:Map[String, List[java.util.Map[String, AnyRef]]]  = batches.filter { case (key, _) => batchIds.contains(key) }
            validBatches.values.foreach(batchList => batchList.foreach(batch => CourseBatchUtil.enrichBatchStatusFromDates(batch)))
            val completedBatchIds = validBatches.filter(batch => 1 != batch._2.head.get(JsonKey.STATUS).asInstanceOf[Integer]).keys.toList.asJava
            val invalidAssessments = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
            val validUserIds = List(requestedBy, requestedFor).filter(p => StringUtils.isNotBlank(p))
            val responseMessage = new java.util.HashMap[String, AnyRef]()
            batchAssessmentList.foreach(input => {
                val batchId = input._1
                if(!invalidBatchIds.contains(batchId) && !completedBatchIds.contains(batchId)) {
                    val userAssessments = updateAssessEventUserid(input._2, requestedBy, requestedFor)
                    userAssessments.foreach(assessments => {
                        val userId = assessments._1
                        if(validUserIds.contains(userId)){
                            assessments._2.foreach(assessment => {
                                syncAssessmentData(assessment, requestContext)
                                responseMessage.put(batchId, JsonKey.SUCCESS)
                            })
                        } else {
                            invalidAssessments.addAll(assessments._2.asJava)
                        }
                    })
                }
                
            })
            if(CollectionUtils.isNotEmpty(completedBatchIds)) responseMessage.put("NOT_A_ON_GOING_BATCH", completedBatchIds)
            if(CollectionUtils.isNotEmpty(invalidBatchIds)) responseMessage.put("BATCH_NOT_EXISTS", invalidBatchIds)
            if(CollectionUtils.isNotEmpty(invalidAssessments)) {
                val map = new java.util.HashMap[String, AnyRef]() {{
                    put("validUserIds", validUserIds)
                    put("invalidAssessments", invalidAssessments)
                    put("ets", System.currentTimeMillis.asInstanceOf[AnyRef])
                }}
                pushInvalidDataToKafka(requestContext, map, "Assessments")
            }
            val response = new Response()
            response.putAll(responseMessage)
            Option(response)
        } else None
    }

    def processContents(contentList: java.util.List[java.util.Map[String, AnyRef]], requestContext: RequestContext, requestedBy: String, requestedFor: String): Option[Response] = {
        if(CollectionUtils.isNotEmpty(contentList)) {
            val batchContentList: Map[String, List[java.util.Map[String, AnyRef]]] = contentList.filter(event => StringUtils.isNotBlank(event.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String])).toList.groupBy(event => event.get(JsonKey.BATCH_ID).asInstanceOf[String])
            val batchIds = batchContentList.keySet.toList.asJava
            val batches:Map[String, List[java.util.Map[String, AnyRef]]] = getBatches(requestContext ,new java.util.ArrayList[String](batchIds), null).toList.groupBy(batch => batch.get(JsonKey.BATCH_ID).asInstanceOf[String])
            val invalidBatchIds = batchContentList.keySet.diff(batches.keySet).toList.asJava
            val validBatches:Map[String, List[java.util.Map[String, AnyRef]]]  = batches.filter { case (key, _) => batchIds.contains(key) }
            validBatches.values.foreach(batchList => batchList.foreach(batch => CourseBatchUtil.enrichBatchStatusFromDates(batch)))
            val completedBatchIds = validBatches.filter(batch => 1 != batch._2.head.get(JsonKey.STATUS).asInstanceOf[Integer]).keys.toList.asJava
            val responseMessage = new java.util.HashMap[String, AnyRef]()
            val invalidContents = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
            val validUserIds = List(requestedBy, requestedFor).filter(p => StringUtils.isNotBlank(p))
            batchContentList.foreach(input => {
                val batchId = input._1
                if(!invalidBatchIds.contains(batchId) && !completedBatchIds.contains(batchId)) {
                    val userContents = getDataGroupedByUserId(input._2, requestedBy, requestedFor)
                    userContents.foreach(entry => {
                        val userId = entry._1
                        if(validUserIds.contains(userId)) {
                            val courseId = if (entry._2.head.containsKey(JsonKey.COURSE_ID)) entry._2.head.getOrDefault(JsonKey.COURSE_ID, "").asInstanceOf[String] else entry._2.head.getOrDefault(JsonKey.COLLECTION_ID, "").asInstanceOf[String]
                            if(entry._2.head.containsKey(JsonKey.COLLECTION_ID)) entry._2.head.remove(JsonKey.COLLECTION_ID)
                            val contentIds = entry._2.map(e => e.getOrDefault(JsonKey.CONTENT_ID, "").asInstanceOf[String]).distinct.asJava
                            val existingContents = getContentsConsumption(userId, courseId, contentIds, batchId, requestContext).groupBy(x => x.get("contentId").asInstanceOf[String]).map(e => e._1 -> e._2.toList.head).toMap
                            val contents:List[java.util.Map[String, AnyRef]] = entry._2.toList.map(inputContent => {
                                val existingContent = existingContents.getOrElse(inputContent.get("contentId").asInstanceOf[String], new java.util.HashMap[String, AnyRef])
                                val m = CassandraUtil.changeCassandraColumnMapping(processContentConsumption(inputContent, existingContent, userId))
                                // ucc columns were renamed courseid->collectionid, batchid->contextid; the global column map still yields courseid/batchid
                                Option(m.remove("courseid")).foreach(v => m.put("collectionid", v))
                                Option(m.remove("batchid")).foreach(v => m.put("contextid", v))
                                m
                            })
                            cassandraOperation.batchInsertLogged(consumptionDBInfo.getKeySpace, consumptionDBInfo.getTableName, contents, requestContext)
                            val updateData = getLatestReadDetails(userId, batchId, contents)
                            cassandraOperation.updateRecordV2(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, updateData._1, updateData._2, true, requestContext)
                            contentIds.map(id => responseMessage.put(id,JsonKey.SUCCESS))
                            val useActivityAggregator = ProjectUtil.getConfigValue("enable_activity_aggregator_actor")
                            if (StringUtils.isNotBlank(useActivityAggregator) && useActivityAggregator.equalsIgnoreCase("true")) {
                                logger.info(requestContext, s"ContentConsumptionActor: Routing to ActivityAggregatorActor for userId: $userId, batchId: $batchId, courseId: $courseId")
                                callActivityAggregatorActor(requestContext, userId, batchId, courseId, entry._2.asJava)
                            } else {
                                logger.info(requestContext, s"ContentConsumptionActor: Using legacy Kafka workflow for userId: $userId, batchId: $batchId, courseId: $courseId")
                                pushInstructionEvent(requestContext, userId, batchId, courseId, contents.asJava)
                            }

                        } else {
                            logger.info(requestContext, "ContentConsumptionActor: addContent : User Id is invalid : " + userId)
                            invalidContents.addAll(entry._2.asJava)
                        }
                    })
                   
                }
            })
            if(CollectionUtils.isNotEmpty(completedBatchIds)) responseMessage.put("NOT_A_ON_GOING_BATCH", completedBatchIds)
            if(CollectionUtils.isNotEmpty(invalidBatchIds)) responseMessage.put("BATCH_NOT_EXISTS", invalidBatchIds)
            if(CollectionUtils.isNotEmpty(invalidContents)) {
                val map = new java.util.HashMap[String, AnyRef]() {{
                    put("validUserIds", validUserIds)
                    put("invalidContents", invalidContents)
                    put("ets", System.currentTimeMillis.asInstanceOf[AnyRef])
                }}
                pushInvalidDataToKafka(requestContext, map, "Contents")
            }
            val response = new Response()
            response.putAll(responseMessage)
            Option(response)
        } else None
    }

    def getDataGroupedByUserId(data: List[java.util.Map[String, AnyRef]], requestedBy: String, requestedFor: String) = {
        val primaryUserId = if(StringUtils.isNotBlank(requestedFor)) requestedFor else requestedBy
        val updatedData: List[java.util.Map[String, AnyRef]] = data.map(f => {
            val userId = f.getOrDefault(JsonKey.USER_ID, "").asInstanceOf[String]
            if(StringUtils.isBlank(userId))
                f.put(JsonKey.USER_ID, primaryUserId)
            f
        })
        updatedData.groupBy(d => d.get(JsonKey.USER_ID).asInstanceOf[String])
    }

    def syncAssessmentData(assessment: java.util.Map[String, AnyRef], requestContext: RequestContext): Unit = {
        val useDirectAggregation = java.lang.Boolean.parseBoolean(ProjectUtil.getConfigValue("assessment_direct_aggregation_enabled"))
        if (useDirectAggregation) {
            logger.info(requestContext, "Using assessment aggregator module")
            val attemptId = AssessmentAuditRecorder.record(assessment, questionUDTType, requestContext)
            assessment.put(JsonKey.ATTEMPT_ID, attemptId)
            val request = createAssessmentRequest(assessment, requestContext)
            assessmentAggregatorActor ! request
            logger.info(requestContext, s"Assessment sent to aggregator (async): attemptId=$attemptId")
        } else {
            logger.info(requestContext, "Using Kafka-based assessment aggregation")
            val topic = ProjectUtil.getConfigValue("kafka_assessment_topic")
            if (StringUtils.isNotBlank(topic)) KafkaClient.send(mapper.writeValueAsString(assessment), topic)
            else throw new ProjectCommonException("BE_JOB_REQUEST_EXCEPTION", "Invalid topic id.", ResponseCode.CLIENT_ERROR.getResponseCode)
        }
    }
    
    private def createAssessmentRequest(assessment: java.util.Map[String, AnyRef], requestContext: RequestContext): Request = {
        val request = new Request()
        request.setRequestContext(requestContext)
        request.setOperation("aggregateAssessment")
        val fields = List(JsonKey.ATTEMPT_ID -> "attemptId", JsonKey.USER_ID -> "userId", JsonKey.COURSE_ID -> "courseId", JsonKey.BATCH_ID -> "batchId", JsonKey.CONTENT_ID -> "contentId")
        fields.foreach { case (k, target) => request.put(target, assessment.get(k)) }
        val ts = Option(assessment.get("assessmentTimestamp")).orElse(Option(assessment.get(JsonKey.ASSESSMENT_TS))).getOrElse(System.currentTimeMillis().asInstanceOf[AnyRef])
        request.put("assessmentTimestamp", ts)
        Option(assessment.get("events")).foreach(e => request.getRequest.put("events", e))
        logger.info(requestContext, s"Extracted attemptId: ${assessment.get(JsonKey.ATTEMPT_ID)}, courseId: ${assessment.get(JsonKey.COURSE_ID)}, batchId: ${assessment.get(JsonKey.BATCH_ID)}, events: ${if (assessment.get("events") != null) "present" else "null"}")
        request
    }

    private def pushInvalidDataToKafka(requestContext: RequestContext, data: java.util.Map[String, AnyRef], dataType: String): Unit = {
        logger.info(requestContext, "LearnerStateUpdater - Invalid " + dataType, null, data)
        val topic = ProjectUtil.getConfigValue("kafka_topics_contentstate_invalid")
        try {
            val event = mapper.writeValueAsString(data)
            KafkaClient.send(event, topic)
        } catch {
            case t: Throwable =>
                t.printStackTrace()
        }
    }

    def getContentsConsumption(userId: String, courseId : String, contentIds: java.util.List[String], batchId: String, requestContext: RequestContext):java.util.List[java.util.Map[String, AnyRef]] = {
        val filters = new java.util.HashMap[String, AnyRef]() {{
            put("userid", userId)
            put("collectionid", courseId)
            put("contextid", batchId)
            if(CollectionUtils.isNotEmpty(contentIds))
                put("contentid", contentIds)
        }}
        val response = cassandraOperation.getRecords(consumptionDBInfo.getKeySpace, consumptionDBInfo.getTableName, filters, null, requestContext)
        response.getResult.getOrDefault(JsonKey.RESPONSE, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    }

    def processContentConsumption(inputContent: java.util.Map[String, AnyRef], existingContent: java.util.Map[String, AnyRef], userId: String) = {
        val inputStatus = inputContent.getOrDefault(JsonKey.STATUS, 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
        val updatedContent = new java.util.HashMap[String, AnyRef]()
        updatedContent.putAll(inputContent)
        val parsedMap = new java.util.HashMap[String, AnyRef]()
        jsonFields.foreach(field =>
            if(inputContent.containsKey(field)) {
                parsedMap.put(field, mapper.writeValueAsString(inputContent.get(field)))
            }
        )
        updatedContent.putAll(parsedMap)
        val inputCompletedTime = parseDate(inputContent.getOrDefault(JsonKey.LAST_COMPLETED_TIME, "").asInstanceOf[String])
        val inputAccessTime = parseDate(inputContent.getOrDefault(JsonKey.LAST_ACCESS_TIME, "").asInstanceOf[String])
        if(MapUtils.isNotEmpty(existingContent)) {
            val existingAccessTime = if(parseDate(existingContent.get(JsonKey.LAST_ACCESS_TIME).asInstanceOf[Date]) == null) parseDate(existingContent.getOrDefault(JsonKey.OLD_LAST_ACCESS_TIME, "").asInstanceOf[String]) else parseDate(existingContent.get(JsonKey.LAST_ACCESS_TIME).asInstanceOf[Date])
            updatedContent.put(JsonKey.LAST_ACCESS_TIME, compareTime(existingAccessTime, inputAccessTime))
            val inputProgress = inputContent.getOrDefault(JsonKey.PROGRESS, 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
            val existingProgress = Option(existingContent.getOrDefault(JsonKey.PROGRESS, 0.asInstanceOf[AnyRef]).asInstanceOf[Number]).getOrElse(0.asInstanceOf[Number]).intValue()
            updatedContent.put(JsonKey.PROGRESS, List(inputProgress, existingProgress).max.asInstanceOf[AnyRef])
            val existingStatus = Option(existingContent.getOrDefault(JsonKey.STATUS, 0.asInstanceOf[AnyRef]).asInstanceOf[Number]).getOrElse(0.asInstanceOf[Number]).intValue()
            val existingCompletedTime = if (parseDate(existingContent.get(JsonKey.LAST_COMPLETED_TIME).asInstanceOf[Date]) == null) parseDate(existingContent.getOrDefault(JsonKey.OLD_LAST_COMPLETED_TIME, "").asInstanceOf[String]) else parseDate(existingContent.get(JsonKey.LAST_COMPLETED_TIME).asInstanceOf[Date])
            if(inputStatus >= existingStatus) {
                if(inputStatus >= 2) {
                    updatedContent.put(JsonKey.STATUS, 2.asInstanceOf[AnyRef])
                    updatedContent.put(JsonKey.PROGRESS, 100.asInstanceOf[AnyRef])
                    updatedContent.put(JsonKey.LAST_COMPLETED_TIME, compareTime(existingCompletedTime, inputCompletedTime))
                }
            } else {
                updatedContent.put(JsonKey.STATUS, existingStatus.asInstanceOf[AnyRef])
            }
        } else {
            if(inputStatus >= 2) {
                updatedContent.put(JsonKey.PROGRESS, 100.asInstanceOf[AnyRef])
                updatedContent.put(JsonKey.LAST_COMPLETED_TIME, compareTime(null, inputCompletedTime))
            } else {
                updatedContent.put(JsonKey.PROGRESS, 0.asInstanceOf[AnyRef])
            }
            updatedContent.put(JsonKey.LAST_ACCESS_TIME, compareTime(null, inputAccessTime))
        }
        updatedContent.put(JsonKey.LAST_UPDATED_TIME, ProjectUtil.getTimeStamp)
        updatedContent.put(JsonKey.USER_ID, userId)
        updatedContent
    }

    def parseDate(dateString: String) = {
        if(StringUtils.isNotBlank(dateString) && !StringUtils.equalsIgnoreCase(JsonKey.NULL, dateString)) {
            dateFormatter.parse(dateString)
        } else null
    }

    def parseDate(date: Date) = {
        if(date != null) {
            dateFormatter.parse(dateFormatter.format(date))
        } else null
    }

    def compareTime(existingTime: java.util.Date, inputTime: java.util.Date): Date = {
        if (null == existingTime && null == inputTime) {
            ProjectUtil.getTimeStamp
        } else if (null == existingTime) inputTime
        else if (null == inputTime) existingTime
        else {
            if (inputTime.after(existingTime)) inputTime
            else existingTime
        }
    }

    def getLatestReadDetails(userId: String, batchId: String, contents: List[java.util.Map[String, AnyRef]]) = {
       val lastAccessContent: java.util.Map[String, AnyRef] = contents.groupBy(x => x.getOrDefault(JsonKey.LAST_ACCESS_TIME_KEY, null).asInstanceOf[Date]).maxBy(_._1)._2.get(0)
       val updateMap = new java.util.HashMap[String, AnyRef] () {{
            put("lastreadcontentid", lastAccessContent.get(JsonKey.CONTENT_ID_KEY))
            put("lastreadcontentstatus", lastAccessContent.get("status"))
            put(JsonKey.LAST_CONTENT_ACCESS_TIME, lastAccessContent.get(JsonKey.LAST_ACCESS_TIME_KEY))

       }}
      val selectMap = new util.HashMap[String, AnyRef]() {{
        put("batchId", batchId)
        put("userId", userId)
        put("courseId", lastAccessContent.get(JsonKey.COURSE_ID_KEY))
      }}
      (selectMap, updateMap)
    }

    @throws[Exception]
    private def pushInstructionEvent(requestContext: RequestContext, userId: String, batchId: String, courseId: String, contents: java.util.List[java.util.Map[String, AnyRef]]): Unit = {
        val data = new java.util.HashMap[String, AnyRef]
        data.put(CourseJsonKey.ACTOR, new java.util.HashMap[String, AnyRef]() {{
            put(JsonKey.ID, InstructionEvent.BATCH_USER_STATE_UPDATE.getActorId)
            put(JsonKey.TYPE, InstructionEvent.BATCH_USER_STATE_UPDATE.getActorType)
        }})
        data.put(CourseJsonKey.OBJECT, new java.util.HashMap[String, AnyRef]() {{
            put(JsonKey.ID, batchId + CourseJsonKey.UNDERSCORE + userId)
            put(JsonKey.TYPE, InstructionEvent.BATCH_USER_STATE_UPDATE.getType)
        }})
        data.put(CourseJsonKey.ACTION, InstructionEvent.BATCH_USER_STATE_UPDATE.getAction)
        val contentsMap = contents.map(c => new java.util.HashMap[String, AnyRef]() {{
            put(JsonKey.CONTENT_ID, c.get(JsonKey.CONTENT_ID_KEY))
            put(JsonKey.STATUS, c.get(JsonKey.STATUS))
        }}).asJava
        data.put(CourseJsonKey.E_DATA, new java.util.HashMap[String, AnyRef]() {{
            put(JsonKey.USER_ID, userId)
            put(JsonKey.BATCH_ID, batchId)
            put(JsonKey.COURSE_ID, courseId)
            put(JsonKey.CONTENTS, contentsMap)
            put(CourseJsonKey.ACTION, InstructionEvent.BATCH_USER_STATE_UPDATE.getAction)
            put(CourseJsonKey.ITERATION, 1.asInstanceOf[AnyRef])
        }})
        val topic = ProjectUtil.getConfigValue("kafka_topics_instruction")
        logger.info(requestContext,"LearnerStateUpdateActor: pushInstructionEvent :Event Data " + data + " and Topic " + topic)
        if(pushTokafkaEnabled)
            InstructionEventGenerator.pushInstructionEvent(userId, topic, data)
    }

    @throws[Exception]
    private def callActivityAggregatorActor(requestContext: RequestContext, userId: String, batchId: String, courseId: String, contents: java.util.List[java.util.Map[String, AnyRef]]): Unit = {
        logger.info(requestContext, s"ContentConsumptionActor: Calling ActivityAggregatorActor for userId: $userId, batchId: $batchId, courseId: $courseId")
        
        val activityRequest = new Request()
        activityRequest.setOperation("updateActivityAggregates")
        activityRequest.setRequestContext(requestContext)
        activityRequest.put(JsonKey.USER_ID, userId)
        activityRequest.put(JsonKey.BATCH_ID, batchId)
        activityRequest.put(JsonKey.COURSE_ID, courseId)
        activityRequest.put(JsonKey.CONTENTS, contents)
        activityAggregatorActor ! activityRequest
    }

    // ProjectUtil.getConfigValue = env var first, then properties file.
    private def isViewerEnabled: Boolean =
        java.lang.Boolean.parseBoolean(ProjectUtil.getConfigValue("viewer_enabled"))

    // Transport for the viewer adapter: monolith -> in-JVM actor ask; distributed -> HTTP (default monolith).
    private def isMonolith: Boolean = !"distributed".equalsIgnoreCase(ProjectUtil.getConfigValue("deployment_mode"))
    private val viewerAskTimeoutMs: Long =
        Option(ProjectUtil.getConfigValue("viewer_ask_timeout_ms")).filter(StringUtils.isNotBlank).map(_.trim.toLong).getOrElse(30000L)
    private implicit val viewerAskTimeout: Timeout = Timeout(viewerAskTimeoutMs.millis)
    private def viewerBaseUrl: String =
        Option(ProjectUtil.getConfigValue("viewer_service_base_url")).filter(StringUtils.isNotBlank).getOrElse("http://viewer-service:9000")
    private def viewerHeaders(token: String): java.util.Map[String, String] =
        new java.util.HashMap[String, String]() {{
            put("Content-Type", "application/json")
            Option(token).filter(StringUtils.isNotBlank).foreach(t => put("x-authenticated-user-token", t))
        }}
    // Blocks until the viewer actor replies (rollup stays async inside the viewer); mirrors the old HTTP await.
    private def viewerAsk(actorName: String, operation: String, body: java.util.Map[String, AnyRef], ctx: RequestContext): AnyRef = {
        val req = new Request(); req.setRequestContext(ctx); req.setOperation(operation); req.setRequest(body)
        Await.result(context.actorSelection("/user/" + actorName) ? req, viewerAskTimeoutMs.millis).asInstanceOf[AnyRef]
    }
    /** Write op (view/start|end, assessment/submit): monolith asks the in-JVM actor, distributed POSTs. */
    private def viewerWrite(actorName: String, httpApi: String, operation: String,
                            body: java.util.Map[String, AnyRef], token: String, ctx: RequestContext): Boolean =
        // success = a Response reply / responseCode OK; onReceiveException replies with the (non-null) exception, so a bare null-check would report failures as SUCCESS
        if (isMonolith) viewerAsk(actorName, operation, body, ctx) match { case _: Response => true; case _ => false }
        else {
            val resp = HttpClientUtil.post(viewerBaseUrl + httpApi,
                mapper.writeValueAsString(new java.util.HashMap[String, AnyRef]() {{ put(JsonKey.REQUEST, body) }}), viewerHeaders(token), ctx)
            StringUtils.isNotBlank(resp) && (try "OK" == mapper.readTree(resp).path("responseCode").asText("") catch { case _: Exception => false })
        }
    /** Read op (view/read): viewer returns the spec `contents`; ViewerRowMapper maps each item back to the internal ucc row so getConsumption is unchanged. */
    private def viewerRead(actorName: String, httpApi: String, operation: String,
                           body: java.util.Map[String, AnyRef], token: String, ctx: RequestContext): java.util.List[java.util.Map[String, AnyRef]] = {
        val empty = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
        val items: java.util.List[java.util.Map[String, AnyRef]] =
            if (isMonolith) viewerAsk(actorName, operation, body, ctx) match {
                case r: Response => r.getResult.getOrDefault("contents", empty).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                case _ => empty
            } else {
                val responseStr = HttpClientUtil.post(viewerBaseUrl + httpApi,
                    mapper.writeValueAsString(new java.util.HashMap[String, AnyRef]() {{ put(JsonKey.REQUEST, body) }}), viewerHeaders(token), ctx)
                if (StringUtils.isBlank(responseStr)) empty
                else mapper.convertValue(mapper.readTree(responseStr).path("result").path("contents"),
                    classOf[java.util.List[java.util.Map[String, AnyRef]]])
            }
        val rows = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
        items.asScala.foreach { item =>
            val row = ViewerRowMapper.toUccRow(item, mapper)
            coerceUccRowTypes(row)
            rows.add(row)
        }
        rows
    }

    // dispatch content/state to the viewer (status>=2 -> viewEnd else viewStart), keeping the legacy invalid/closed-batch guards
    private def delegateContentsToViewer(contentList: java.util.List[java.util.Map[String, AnyRef]],
                                         originalRequest: Request,
                                         requestedBy: String, requestedFor: String): Option[Response] = {
        if (CollectionUtils.isEmpty(contentList)) return None
        val ctx = originalRequest.getRequestContext
        val userId = if (StringUtils.isNotBlank(requestedFor)) requestedFor else requestedBy
        val token = originalRequest.getContext.get(JsonKey.X_AUTH_TOKEN).asInstanceOf[String]
        val responseMessage = new java.util.HashMap[String, AnyRef]()

        val batchContentList: Map[String, List[java.util.Map[String, AnyRef]]] =
            contentList.asScala.filter(c => StringUtils.isNotBlank(c.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String])).toList.groupBy(c => c.get(JsonKey.BATCH_ID).asInstanceOf[String])
        val batchIds = batchContentList.keySet.toList.asJava
        val batches: Map[String, List[java.util.Map[String, AnyRef]]] =
            getBatches(ctx, new java.util.ArrayList[String](batchIds), null).toList.groupBy(batch => batch.get(JsonKey.BATCH_ID).asInstanceOf[String])
        val invalidBatchIds = batchContentList.keySet.diff(batches.keySet).toList.asJava
        val validBatches: Map[String, List[java.util.Map[String, AnyRef]]] = batches.filter { case (key, _) => batchIds.contains(key) }
        validBatches.values.foreach(batchList => batchList.foreach(batch => CourseBatchUtil.enrichBatchStatusFromDates(batch)))
        val completedBatchIds = validBatches.filter(batch => 1 != batch._2.head.get(JsonKey.STATUS).asInstanceOf[Integer]).keys.toList.asJava

        batchContentList.foreach { case (batchId, contents) =>
            if (!invalidBatchIds.contains(batchId) && !completedBatchIds.contains(batchId)) {
                contents.foreach(c => {
                    val contentId = c.get(JsonKey.CONTENT_ID).asInstanceOf[String]
                    try {
                        val status = c.getOrDefault(JsonKey.STATUS, 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
                        val courseId = Option(c.get(JsonKey.COURSE_ID).asInstanceOf[String]).filter(StringUtils.isNotBlank)
                          .getOrElse(c.get(JsonKey.COLLECTION_ID).asInstanceOf[String])
                        val (op, api) = if (status >= 2) ("viewEnd", "/v1/view/end") else ("viewStart", "/v1/view/start")
                        val body = new java.util.HashMap[String, AnyRef]() {{
                            put("contentId", contentId)
                            put("courseId", courseId)
                            put("batchId", batchId)
                            put(JsonKey.USER_ID, userId)
                            Option(c.get("progressdetails")).orElse(Option(c.get("progressDetails"))).foreach(pd => put("progressDetails", pd))
                            Option(c.get(JsonKey.PROGRESS)).foreach(p => put(JsonKey.PROGRESS, p))
                            Option(c.get(JsonKey.VIEW_COUNT)).foreach(v => put(JsonKey.VIEW_COUNT, v))
                            Option(c.get(JsonKey.LAST_ACCESS_TIME)).foreach(v => put(JsonKey.LAST_ACCESS_TIME, v))
                            Option(c.get(JsonKey.LAST_COMPLETED_TIME)).foreach(v => put(JsonKey.LAST_COMPLETED_TIME, v))
                        }}
                        responseMessage.put(contentId, if (viewerWrite("view-consumption-actor", api, op, body, token, ctx)) JsonKey.SUCCESS else "FAILED")
                    } catch {
                        case ex: Exception =>
                            logger.error(ctx, s"delegateContentsToViewer failed for contentId=$contentId: ${ex.getMessage}", ex)
                            responseMessage.put(contentId, "FAILED")
                    }
                })
            }
        }
        if (CollectionUtils.isNotEmpty(completedBatchIds)) responseMessage.put("NOT_A_ON_GOING_BATCH", completedBatchIds)
        if (CollectionUtils.isNotEmpty(invalidBatchIds)) responseMessage.put("BATCH_NOT_EXISTS", invalidBatchIds)
        val response = new Response(); response.putAll(responseMessage); Option(response)
    }

    // dispatch assessment submissions to the viewer (/v1/assessment/submit -> viewAssess: score + status=2 + rollup),
    // applying the same invalid/closed-batch guards as delegateContentsToViewer
    private def delegateAssessmentsToViewer(assessmentEvents: java.util.List[java.util.Map[String, AnyRef]],
                                            originalRequest: Request,
                                            requestedBy: String, requestedFor: String): Option[Response] = {
        if (CollectionUtils.isEmpty(assessmentEvents)) return None
        val ctx = originalRequest.getRequestContext
        val userId = if (StringUtils.isNotBlank(requestedFor)) requestedFor else requestedBy
        val token = originalRequest.getContext.get(JsonKey.X_AUTH_TOKEN).asInstanceOf[String]
        val byBatch: Map[String, List[java.util.Map[String, AnyRef]]] = assessmentEvents.asScala
          .filter(e => StringUtils.isNotBlank(e.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String])).toList
          .groupBy(_.get(JsonKey.BATCH_ID).asInstanceOf[String])
        val batchIds = byBatch.keySet.toList.asJava
        val batches: Map[String, List[java.util.Map[String, AnyRef]]] =
          getBatches(ctx, new java.util.ArrayList[String](batchIds), null).toList.groupBy(_.get(JsonKey.BATCH_ID).asInstanceOf[String])
        val invalidBatchIds = byBatch.keySet.diff(batches.keySet).toList.asJava
        batches.values.foreach(bl => bl.foreach(b => CourseBatchUtil.enrichBatchStatusFromDates(b)))
        val completedBatchIds = batches.filter(b => 1 != b._2.head.get(JsonKey.STATUS).asInstanceOf[Integer]).keys.toList.asJava
        val responseMessage = new java.util.HashMap[String, AnyRef]()
        byBatch.foreach { case (batchId, events) =>
            if (!invalidBatchIds.contains(batchId) && !completedBatchIds.contains(batchId)) {
                events.foreach(a => {
                    try {
                        val courseId = a.get(JsonKey.COURSE_ID).asInstanceOf[String]
                        val evs = a.getOrDefault(JsonKey.ASSESSMENT_EVENTS_KEY, new java.util.ArrayList[java.util.Map[String, AnyRef]]())
                        val body = new java.util.HashMap[String, AnyRef]() {{
                            put("contentId", a.get(JsonKey.CONTENT_ID))
                            put("courseId", courseId)
                            put("batchId", batchId)
                            put(JsonKey.USER_ID, userId)
                            put(JsonKey.ASSESSMENT_EVENTS, evs)
                            // forward client attemptId/assessmentTs (documented contract) so replays upsert and offline-sync ts is preserved
                            Option(a.get(JsonKey.ATTEMPT_ID)).foreach(v => put(JsonKey.ATTEMPT_ID, v))
                            Option(a.get(JsonKey.ASSESSMENT_TS)).orElse(Option(a.get("assessmentTimestamp"))).foreach(v => put(JsonKey.ASSESSMENT_TS, v))
                        }}
                        responseMessage.put(batchId, if (viewerWrite("view-consumption-actor", "/v1/assessment/submit", "viewAssess", body, token, ctx)) JsonKey.SUCCESS else "FAILED")
                    } catch {
                        case ex: Exception =>
                            logger.error(ctx, s"delegateAssessmentsToViewer failed for batchId=$batchId: ${ex.getMessage}", ex)
                            responseMessage.put(batchId, "FAILED")
                    }
                })
            }
        }
        if (CollectionUtils.isNotEmpty(completedBatchIds)) responseMessage.put("NOT_A_ON_GOING_BATCH", completedBatchIds)
        if (CollectionUtils.isNotEmpty(invalidBatchIds)) responseMessage.put("BATCH_NOT_EXISTS", invalidBatchIds)
        val response = new Response(); response.putAll(responseMessage); Option(response)
    }

    // content/state/read adapter: fetch ucc rows via viewRead; same row shape as getContentsConsumption so post-processing is unchanged
    private def readContentsFromViewer(userId: String, courseId: String, batchId: String,
                                       contentIds: java.util.List[String], originalRequest: Request): java.util.List[java.util.Map[String, AnyRef]] = {
        val ctx = originalRequest.getRequestContext
        val token = originalRequest.getContext.get(JsonKey.X_AUTH_TOKEN).asInstanceOf[String]
        val body = new java.util.HashMap[String, AnyRef]() {{
            put(JsonKey.USER_ID, userId)
            put("courseId", courseId)
            put("batchId", batchId)
            if (CollectionUtils.isNotEmpty(contentIds)) put("contentId", contentIds)
        }}
        try viewerRead("view-consumption-actor", "/v1/view/read", "viewRead", body, token, ctx)
        catch {
            case ex: Exception =>
                logger.error(ctx, s"readContentsFromViewer failed for userId=$userId courseId=$courseId: ${ex.getMessage}", ex)
                new java.util.ArrayList[java.util.Map[String, AnyRef]]()
        }
    }

    // camelCase keys (createResponse maps DB cols to camelCase) — the shape viewRead returns over HTTP
    private val uccTimestampCols = Set(JsonKey.LAST_ACCESS_TIME, JsonKey.LAST_COMPLETED_TIME, "lastUpdatedTime", "dateTime")
    private val uccIntCols = Set(JsonKey.STATUS, JsonKey.PROGRESS, "completedCount", "viewCount")

    private def coerceUccRowTypes(row: java.util.Map[String, AnyRef]): Unit = {
        uccTimestampCols.foreach { c => val d = toDate(row.get(c)); if (d != null) row.put(c, d) }
        uccIntCols.foreach { c => row.get(c) match { case n: Number => row.put(c, Integer.valueOf(n.intValue())); case _ => } }
        row.get("completionPercentage") match { case n: Number => row.put("completionPercentage", java.lang.Float.valueOf(n.floatValue())); case _ => }
    }

    /** Reconstruct a java.util.Date from the viewer's JSON form (epoch-millis number, numeric string, or ISO-8601). */
    private def toDate(v: AnyRef): java.util.Date = v match {
        case null => null
        case d: java.util.Date => d
        case n: Number => new java.util.Date(n.longValue())
        case s: String if StringUtils.isNotBlank(s) =>
            try new java.util.Date(s.toLong)
            catch { case _: Throwable => try java.util.Date.from(java.time.Instant.parse(s)) catch { case _: Throwable => null } }
        case _ => null
    }

    def getConsumption(request: Request): Unit = {
        val userId = request.get(JsonKey.USER_ID).asInstanceOf[String]
        val batchId = request.get(JsonKey.BATCH_ID).asInstanceOf[String]
        val courseId = request.get(JsonKey.COURSE_ID).asInstanceOf[String]
        val contentIds = request.getRequest.getOrDefault(JsonKey.CONTENT_IDS, new java.util.ArrayList[String]()).asInstanceOf[java.util.List[String]]
        val fields = request.getRequest.getOrDefault(JsonKey.FIELDS, new java.util.ArrayList[String](){{ add(JsonKey.PROGRESS) }}).asInstanceOf[java.util.List[String]]
        // viewer.enabled -> content-state read served by the Viewer Service (/v1/view/read); same rows, so post-processing is reused unchanged
        val contentsConsumed =
          if (isViewerEnabled) readContentsFromViewer(userId, courseId, batchId, contentIds, request)
          else getContentsConsumption(userId, courseId, contentIds, batchId, request.getRequestContext)
        val response = new Response
        if(CollectionUtils.isNotEmpty(contentsConsumed)) {
            val filteredContents = contentsConsumed.map(m => {
                // surface renamed ucc columns as the courseId/batchId contract (no-op for the viewer path, which already aliases in viewRead)
                Option(m.remove("collectionid")).foreach(v => m.put("courseId", v))
                Option(m.remove("contextid")).foreach(v => m.put("batchId", v))
                ProjectUtil.removeUnwantedFields(m, JsonKey.DATE_TIME, JsonKey.USER_ID, JsonKey.ADDED_BY, JsonKey.LAST_UPDATED_TIME, JsonKey.OLD_LAST_ACCESS_TIME, JsonKey.OLD_LAST_UPDATED_TIME, JsonKey.OLD_LAST_COMPLETED_TIME)
                m.put(JsonKey.COLLECTION_ID, m.getOrDefault(JsonKey.COURSE_ID, ""))
                jsonFields.foreach(field =>
                    if(m.get(field) != null)
                        m.put(field, mapper.readTree(m.get(field).asInstanceOf[String]))
                )
                val formattedMap = JsonUtil.convertWithDateFormat(m, classOf[util.Map[String, Object]], dateFormatter)
                if (fields.contains(JsonKey.ASSESSMENT_SCORE))
                    formattedMap.putAll(mapAsJavaMap(Map(JsonKey.ASSESSMENT_SCORE -> getScore(userId, courseId, m.get("contentId").asInstanceOf[String], batchId, request.getRequestContext))))
                formattedMap
            }).asJava
            response.put(JsonKey.RESPONSE, filteredContents)
        } else {
            response.put(JsonKey.RESPONSE, new java.util.ArrayList[AnyRef]())
        }
        sender().tell(response, self)
    }
    
    //TODO: to be removed once all in scala
    def setCassandraOperation(cassandraOps: CassandraOperation, kafkaEnabled: Boolean): ContentConsumptionActor = {
        pushTokafkaEnabled = kafkaEnabled
        cassandraOperation = cassandraOps
        this
    }

    def getScore(userId: String, courseId: String, contentId: String, batchId: String, requestContext: RequestContext): util.List[util.Map[String, AnyRef]] = {
        val filters = new java.util.HashMap[String, AnyRef]() {
            {
                put("user_id", userId)
                put("collection_id", courseId)
                put("context_id", batchId)
                put("content_id", contentId)
            }
        }
        val fieldsToGet = new java.util.ArrayList[String](){{
            add("attempt_id")
            add("last_attempted_on")
            add("total_max_score")
            add("total_score")
        }}
        val limit = if (StringUtils.isNotBlank(ProjectUtil.getConfigValue("assessment.attempts.limit")))
            (ProjectUtil.getConfigValue("assessment.attempts.limit")).asInstanceOf[Integer] else 25.asInstanceOf[Integer]
        val response = cassandraOperation.getRecordsWithLimit(assessmentAggregatorDBInfo.getKeySpace, assessmentAggregatorDBInfo.getTableName, filters, fieldsToGet, limit, requestContext)
        response.getResult.getOrDefault(JsonKey.RESPONSE, new java.util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    }

    def processEnrolmentSync(request: Request, requestedBy: String, requestedFor: String): Unit = {
        val primaryUserId = if (StringUtils.isNotBlank(requestedFor)) requestedFor else requestedBy
        val userId: String = request.getOrDefault(JsonKey.USER_ID, primaryUserId).asInstanceOf[String]
        val courseId: String = request.getOrDefault(JsonKey.COURSE_ID, "").asInstanceOf[String]
        val batchId: String = request.getOrDefault(JsonKey.BATCH_ID, "").asInstanceOf[String]
        val filters = Map[String, AnyRef]("userid"-> userId, "courseid"-> courseId, "batchid"-> batchId).asJava
        val result = cassandraOperation
          .getRecords(enrolmentDBInfo.getKeySpace, enrolmentDBInfo.getTableName, filters,
              null, request.getRequestContext)
        val resp = result.getResult
          .getOrDefault(JsonKey.RESPONSE, new java.util.ArrayList[java.util.Map[String, AnyRef]])
          .asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        val response = {
            if (CollectionUtils.isNotEmpty(resp)) {
                pushEnrolmentSyncEvent(userId, courseId, batchId)
                successResponse()
            } else {
                new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode,
                    s"""No Enrolment found for userId: $userId, batchId: $batchId, courseId: $courseId""", ResponseCode.CLIENT_ERROR.getResponseCode)
            }
        }
        sender().tell(response, self)
    }

    def pushEnrolmentSyncEvent(userId: String, courseId: String, batchId: String) = {
        val now = System.currentTimeMillis()
        val event =
            s"""{"eid":"BE_JOB_REQUEST","ets":$now,"mid":"LP.$now.${UUID.randomUUID()}"
               |,"actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}}
               |,"object":{"type":"CourseBatchEnrolment","id":"${batchId}_${userId}"},"edata":{"action":"user-enrolment-sync"
               |,"iteration":1,"batchId":"$batchId","userId":"$userId","courseId":"$courseId"}}""".stripMargin
              .replaceAll("\n", "")
        if(pushTokafkaEnabled){
            val topic = ProjectUtil.getConfigValue("kafka_enrolment_sync_topic")
            KafkaClient.send(userId, event, topic)
        }
    }
}
