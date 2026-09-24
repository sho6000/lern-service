package org.sunbird.viewer.actor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sunbird.request.Request

// Viewer accepts both the current keys (courseId/batchId) and the spec keys (collectionId/contextId).
// Current keys take priority so existing callers (content/state delegation) are unaffected.
class ViewerRequestKeysTest extends AnyFlatSpec with Matchers {

  private def req(kv: (String, String)*): Request = {
    val r = new Request; kv.foreach { case (k, v) => r.put(k, v) }; r
  }

  "courseId" should "read the courseId key" in {
    ViewerRequestKeys.courseId(req("courseId" -> "c1")) shouldBe Some("c1")
  }

  it should "fall back to collectionId (spec alias) when courseId absent" in {
    ViewerRequestKeys.courseId(req("collectionId" -> "c1")) shouldBe Some("c1")
  }

  it should "prefer courseId when both are present" in {
    ViewerRequestKeys.courseId(req("courseId" -> "c1", "collectionId" -> "cX")) shouldBe Some("c1")
  }

  it should "be None when neither is present" in {
    ViewerRequestKeys.courseId(req()) shouldBe None
  }

  "batchId" should "read the batchId key" in {
    ViewerRequestKeys.batchId(req("batchId" -> "b1")) shouldBe Some("b1")
  }

  it should "fall back to contextId (spec alias) when batchId absent" in {
    ViewerRequestKeys.batchId(req("contextId" -> "b1")) shouldBe Some("b1")
  }

  it should "prefer batchId when both are present" in {
    ViewerRequestKeys.batchId(req("batchId" -> "b1", "contextId" -> "bX")) shouldBe Some("b1")
  }
}
