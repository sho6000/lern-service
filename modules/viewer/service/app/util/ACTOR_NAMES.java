package util;

import org.sunbird.viewer.actor.ViewConsumptionActor;
import org.sunbird.viewer.actor.ViewerAggregatorActor;
import org.sunbird.viewer.actor.ViewerSummaryActor;

public enum ACTOR_NAMES {
  VIEW_CONSUMPTION_ACTOR(ViewConsumptionActor.class, "view-consumption-actor"),
  VIEWER_AGGREGATOR_ACTOR(ViewerAggregatorActor.class, "viewer-aggregator-actor"),
  VIEWER_SUMMARY_ACTOR(ViewerSummaryActor.class, "viewer-summary-actor");

  private ACTOR_NAMES(Class clazz, String name) {
    actorClass = clazz;
    actorName = name;
  }

  private Class actorClass;
  private String actorName;

  public Class getActorClass() {
    return actorClass;
  }

  public String getActorName() {
    return actorName;
  }
}
