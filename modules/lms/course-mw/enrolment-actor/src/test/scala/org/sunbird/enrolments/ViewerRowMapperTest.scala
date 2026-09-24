package org.sunbird.enrolments

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util

// The read parser: a viewer view.read "content" item must map back to the internal ucc-row shape
// (courseId/batchId + progressdetails STRING) so content/state's getConsumption is unchanged.
class ViewerRowMapperTest extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper

  private def item(): util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]() {{
    put("contentId", "do_456")
    put("collectionId", "do_123")
    put("contextId", "0123")
    put("status", Integer.valueOf(2))
    put("progress", Integer.valueOf(100))
    put("progressDetails", new util.HashMap[String, AnyRef]() {{ put("mimeType", "application/pdf") }})
  }}

  "toUccRow" should "rename spec keys back to the internal ucc row (courseId/batchId)" in {
    val row = ViewerRowMapper.toUccRow(item(), mapper)
    row.get("courseId") shouldBe "do_123"
    row.get("batchId") shouldBe "0123"
    row.get("contentId") shouldBe "do_456"
    row.get("status") shouldBe Integer.valueOf(2)
    row.get("progress") shouldBe Integer.valueOf(100)
    row.containsKey("collectionId") shouldBe false
    row.containsKey("contextId") shouldBe false
  }

  it should "serialize progressDetails object back to a progressdetails JSON string" in {
    val row = ViewerRowMapper.toUccRow(item(), mapper)
    row.containsKey("progressDetails") shouldBe false
    row.get("progressdetails").asInstanceOf[String] should include ("application/pdf")
  }
}
