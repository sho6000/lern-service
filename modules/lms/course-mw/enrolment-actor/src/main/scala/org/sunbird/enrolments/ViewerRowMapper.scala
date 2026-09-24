package org.sunbird.enrolments

import com.fasterxml.jackson.databind.ObjectMapper

import java.util

// Maps a viewer view.read content item back to the internal ucc-row shape so content/state's output is unchanged.
object ViewerRowMapper {

  def toUccRow(item: util.Map[String, AnyRef], mapper: ObjectMapper): util.Map[String, AnyRef] = {
    val row = new util.HashMap[String, AnyRef](item)
    Option(row.remove("collectionId")).foreach(v => row.put("courseId", v))
    Option(row.remove("contextId")).foreach(v => row.put("batchId", v))
    Option(row.remove("progressDetails")).foreach(pd => row.put("progressdetails", mapper.writeValueAsString(pd)))
    row
  }
}
