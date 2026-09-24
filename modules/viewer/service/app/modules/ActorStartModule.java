package modules;

import org.apache.pekko.routing.ConsistentHashingPool;
import org.apache.pekko.routing.ConsistentHashingRouter.ConsistentHashMapper;
import org.apache.pekko.routing.FromConfig;
import org.apache.pekko.routing.RouterConfig;
import com.google.inject.AbstractModule;
import play.libs.pekko.PekkoGuiceSupport;
import util.ACTOR_NAMES;

public class ActorStartModule extends AbstractModule implements PekkoGuiceSupport {

  @Override
  protected void configure() {
    System.out.println("binding actors for dependency injection");
    final RouterConfig config = new FromConfig();

    // viewer-aggregator: PROGRAMMATIC ConsistentHashingPool keyed on userId — serialises one user's
    // roll-ups. A config-only consistent-hashing router has no hash key for plain Request messages
    // and would send them to deadLetters.
    final ConsistentHashMapper userIdHashMapper =
        message -> {
          if (message instanceof org.sunbird.request.Request) {
            Object uid = ((org.sunbird.request.Request) message).get("userId");
            return uid != null ? uid : "";
          }
          return "";
        };

    for (ACTOR_NAMES actor : ACTOR_NAMES.values()) {
      if (ACTOR_NAMES.VIEWER_AGGREGATOR_ACTOR.equals(actor)) {
        bindActor(
            actor.getActorClass(),
            actor.getActorName(),
            props -> props.withRouter(new ConsistentHashingPool(8).withHashMapper(userIdHashMapper))
                .withDispatcher("pekko.actor.viewer-dispatcher"));
      } else {
        bindActor(actor.getActorClass(), actor.getActorName(), props -> props.withRouter(config));
      }
    }
    System.out.println("binding completed");
  }
}
