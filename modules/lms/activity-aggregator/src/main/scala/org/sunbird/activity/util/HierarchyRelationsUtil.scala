package org.sunbird.activity.util

import org.sunbird.cache.util.RedisCacheUtil
import org.sunbird.cassandra.CassandraOperation
import org.sunbird.common.ProjectUtil
import org.sunbird.keys.JsonKey
import org.sunbird.logging.LoggerUtil
import org.sunbird.request.RequestContext

import java.util
import scala.collection.JavaConverters._

class HierarchyRelationsUtil(cassandraOperation: CassandraOperation) {

  private val logger = new LoggerUtil(classOf[HierarchyRelationsUtil])

  private val keyspace = Option(ProjectUtil.getConfigValue("hierarchy_store_keyspace")).getOrElse("dev_hierarchy_store")
  private val tableName = Option(ProjectUtil.getConfigValue("hierarchy_relations_table")).getOrElse("hierarchy_relations")

  private lazy val redisCacheUtil: RedisCacheUtil = new RedisCacheUtil()
  private lazy val hierarchyRelationsRedisIndex: Int =
    Option(ProjectUtil.getConfigValue("hierarchy_relations_redis_index")).map(_.toInt).getOrElse(10)
  private def redisEnabled: Boolean = RedisCacheUtil.isRedisEnabled

  def readFromDB(key: String, requestContext: RequestContext): List[String] =
    HierarchyRelationsUtil.cached(key)(readFromDBUncached(key, requestContext))

  private def readFromDBUncached(key: String, requestContext: RequestContext): List[String] = {
    if (redisEnabled) {
      try {
        val nodes = redisCacheUtil.getList(key, hierarchyRelationsRedisIndex)
        if (nodes.isEmpty) logger.info(requestContext, s"HierarchyRelationsUtil: No data found in Redis for key: $key")
        nodes
      } catch {
        case ex: Exception =>
          logger.error(requestContext, s"HierarchyRelationsUtil: Error reading from Redis for key: $key", ex)
          List.empty
      }
    } else {
      logger.info(requestContext, s"HierarchyRelationsUtil.readFromDB: key: $key, keyspace: $keyspace, table: $tableName")
      try {
        val queryMap = new util.HashMap[String, AnyRef]()
        queryMap.put("relationship_key", key)
        val response = cassandraOperation.getRecordsByProperties(keyspace, tableName, queryMap, requestContext)
        if (response != null && response.getResult != null) {
          val result = response.getResult.get(JsonKey.RESPONSE).asInstanceOf[util.List[util.Map[String, AnyRef]]]
          if (result != null && !result.isEmpty) {
            val row = result.get(0)
            val nodeIds = row.get("node_ids")
            if (nodeIds != null) {
              nodeIds.asInstanceOf[util.List[String]].asScala.toList
            } else {
              logger.info(requestContext, s"HierarchyRelationsUtil: No node_ids found for key: $key")
              List.empty
            }
          } else {
            logger.info(requestContext, s"HierarchyRelationsUtil: No data found in DB for key: $key")
            List.empty
          }
        } else {
          logger.info(requestContext, s"HierarchyRelationsUtil: Null response from DB for key: $key")
          List.empty
        }
      } catch {
        case ex: Exception =>
          logger.error(requestContext, s"HierarchyRelationsUtil: Error reading from DB for key: $key", ex)
          List.empty
      }
    }
  }

  def getLeafNodes(courseId: String, collectionId: String, requestContext: RequestContext): List[String] = {
    val key = s"$courseId:$collectionId:leafnodes"
    logger.info(requestContext, s"HierarchyRelationsUtil: Getting leaf nodes for courseId: $courseId, collectionId: $collectionId")
    val nodes = readFromDB(key, requestContext).distinct
    logger.info(requestContext, s"HierarchyRelationsUtil: Retrieved ${nodes.size} distinct leaf nodes")
    nodes
  }

  def getOptionalNodes(courseId: String, collectionId: String, requestContext: RequestContext): List[String] = {
    val key = s"$courseId:$collectionId:optionalnodes"
    logger.info(requestContext, s"HierarchyRelationsUtil: Getting optional nodes for courseId: $courseId, collectionId: $collectionId")
    val nodes = readFromDB(key, requestContext).distinct
    logger.info(requestContext, s"HierarchyRelationsUtil: Retrieved ${nodes.size} distinct optional nodes")
    nodes
  }

  def getAncestors(courseId: String, contentId: String, requestContext: RequestContext): List[String] = {
    val key = s"$courseId:$contentId:ancestors"
    logger.info(requestContext, s"HierarchyRelationsUtil: Getting ancestors for courseId: $courseId, contentId: $contentId")
    val ancestors = readFromDB(key, requestContext)
    logger.info(requestContext, s"HierarchyRelationsUtil: Retrieved ${ancestors.size} ancestors")
    ancestors
  }

  def getRequiredLeafNodes(courseId: String, collectionId: String, requestContext: RequestContext): List[String] = {
    logger.info(requestContext, s"HierarchyRelationsUtil: Getting required leaf nodes (excluding optional) for courseId: $courseId, collectionId: $collectionId")
    val leafNodes = getLeafNodes(courseId, collectionId, requestContext)
    val optionalNodes = getOptionalNodes(courseId, collectionId, requestContext)
    val required = leafNodes.diff(optionalNodes)
    logger.info(requestContext, s"HierarchyRelationsUtil: Required leaf nodes: ${required.size} (total: ${leafNodes.size}, optional: ${optionalNodes.size})")
    required
  }
}

object HierarchyRelationsUtil {
  def apply(cassandraOperation: CassandraOperation): HierarchyRelationsUtil = new HierarchyRelationsUtil(cassandraOperation)

  // JVM-wide TTL cache of relationship_key -> node_ids; accepts up to TTL of staleness on republish (empty results not cached; ttl=0 disables)
  // ponytail: TTL eviction; go version-keyed/event-driven only if republish-during-consumption bites.
  private val ttlMillis: Long =
    Option(ProjectUtil.getConfigValue("hierarchy_relations_cache_ttl"))
      .filter(_.trim.nonEmpty).map(_.trim.toLong).getOrElse(300L) * 1000L
  private val cache = new java.util.concurrent.ConcurrentHashMap[String, (Long, List[String])]()

  private def cached(key: String)(load: => List[String]): List[String] = {
    if (ttlMillis <= 0) return load
    val now = System.currentTimeMillis()
    val hit = cache.get(key)
    if (hit != null && hit._1 > now) hit._2
    else {
      val v = load
      if (v.nonEmpty) cache.put(key, (now + ttlMillis, v))
      v
    }
  }
}
