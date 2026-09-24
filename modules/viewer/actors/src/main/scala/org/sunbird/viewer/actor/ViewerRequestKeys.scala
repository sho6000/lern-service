package org.sunbird.viewer.actor

import org.apache.commons.lang3.StringUtils
import org.sunbird.request.Request

// accepts both courseId/batchId and the spec's collectionId/contextId; courseId/batchId win so existing callers are unaffected
object ViewerRequestKeys {

  private def value(request: Request, key: String): Option[String] =
    Option(request.get(key)).collect { case s: String if StringUtils.isNotBlank(s) => s }

  def courseId(request: Request): Option[String] = value(request, "courseId").orElse(value(request, "collectionId"))
  def batchId(request: Request): Option[String] = value(request, "batchId").orElse(value(request, "contextId"))
  def contentId(request: Request): String = value(request, "contentId").orNull
}
