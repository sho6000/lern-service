package modules;

import org.apache.pekko.routing.ConsistentHashingPool;
import org.apache.pekko.routing.ConsistentHashingRouter.ConsistentHashMapper;
import org.apache.pekko.routing.FromConfig;
import org.apache.pekko.routing.RouterConfig;
import com.google.inject.AbstractModule;
import play.libs.pekko.PekkoGuiceSupport;
import org.sunbird.logging.LoggerUtil;
// import org.sunbird.health.actor.HealthActor;
import actors.HealthActor;

import org.sunbird.notification.actor.CreateNotificationActor;
import org.sunbird.notification.actor.DeleteNotificationActor;
import org.sunbird.notification.actor.NotificationActor;
import org.sunbird.notification.actor.NotificationTemplateActor;
import org.sunbird.notification.actor.ReadNotificationActor;
import org.sunbird.notification.actor.UpdateNotificationActor;
import org.sunbird.observability.actor.ObservabilityReportActor;
import org.sunbird.request.Request;
import org.sunbird.viewer.actor.ViewConsumptionActor;
import org.sunbird.viewer.actor.ViewerAggregatorActor;
import org.sunbird.viewer.actor.ViewerSummaryActor;

public class LernServiceActorStartModule extends AbstractModule implements PekkoGuiceSupport {
    private static LoggerUtil logger = new LoggerUtil(LernServiceActorStartModule.class);

    // hash key for the viewer aggregator pool = userId (per-user serialization)
    private static final ConsistentHashMapper viewerUserIdHashMapper = message -> {
        if (message instanceof Request) {
            Object uid = ((Request) message).get("userId");
            return uid != null ? uid : "";
        }
        return "";
    };

    @Override
    protected void configure() {
        logger.info("LernServiceActorStartModule: Binding actors for ALL services");
        final RouterConfig config = new FromConfig();

        // 1. Bind UserOrg Actors
        for (util.ACTORS actor : util.ACTORS.values()) {
            bindActor(actor.getActorClass(), actor.getActorName(), props -> props.withRouter(config));
        }
        logger.info("UserOrg actors bound");

        // 2. Bind LMS Actors
        for (util.ACTOR_NAMES actor : util.ACTOR_NAMES.values()) {
            bindActor(actor.getActorClass(), actor.getActorName(), props -> props.withRouter(config));
        }
        logger.info("LMS actors bound");

        // 3. Bind Notification Actors
        // Notification service doesn't use an Enum for actor names in the same way, need to bind manually based on its ActorStartModule or create an adapter
        // Checking Notification ActorStartModule logic:
        bindActor(HealthActor.class, "HealthActor", props -> props.withRouter(config));
        bindActor(NotificationActor.class, "NotificationActor", props -> props.withRouter(config));
        bindActor(CreateNotificationActor.class, "CreateNotificationActor", props -> props.withRouter(config));
        bindActor(ReadNotificationActor.class, "ReadNotificationActor", props -> props.withRouter(config));
        bindActor(UpdateNotificationActor.class, "UpdateNotificationActor", props -> props.withRouter(config));
        bindActor(DeleteNotificationActor.class, "DeleteNotificationActor", props -> props.withRouter(config));
        bindActor(NotificationTemplateActor.class, "NotificationTemplateActor", props -> props.withRouter(config));
        
        logger.info("Notification actors bound");

        // 4. Bind Observability Actors
        bindActor(ObservabilityReportActor.class, "observability-report-actor", props -> props.withRouter(config));
        logger.info("Observability actors bound");

        // 5. Bind Viewer Actors (served by this monolith's /v1/view/* routes)
        bindActor(ViewConsumptionActor.class, "view-consumption-actor", props -> props.withRouter(config));
        bindActor(ViewerSummaryActor.class, "viewer-summary-actor", props -> props.withRouter(config));
        // aggregator: consistent-hashing keyed on userId serialises one user's roll-ups (config router
        // can't supply a hash key for plain Request messages -> would deadLetter, hence programmatic).
        bindActor(ViewerAggregatorActor.class, "viewer-aggregator-actor",
            props -> props.withDispatcher("pekko.actor.viewer-dispatcher")
                .withRouter(new ConsistentHashingPool(8).withHashMapper(viewerUserIdHashMapper)));
        logger.info("Viewer actors bound");

        logger.info("LernServiceActorStartModule: All actors bound successfully");
    }
}
