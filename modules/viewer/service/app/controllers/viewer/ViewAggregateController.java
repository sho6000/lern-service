package controllers.viewer;

import controllers.BaseController;
import org.apache.pekko.actor.ActorRef;
import org.sunbird.exception.ProjectCommonException;
import org.sunbird.keys.JsonKey;
import org.sunbird.request.Request;
import org.sunbird.response.ResponseCode;
import play.mvc.Http;
import play.mvc.Result;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// idempotent resync -> viewer-aggregator-actor "aggregate"; recomputes a learner's roll-up from user_content_consumption
public class ViewAggregateController extends BaseController {

    private final ActorRef viewerAggregatorActor;

    @Inject
    public ViewAggregateController(@Named("viewer-aggregator-actor") ActorRef viewerAggregatorActor) {
        this.viewerAggregatorActor = viewerAggregatorActor;
    }

    public CompletionStage<Result> agg(Http.Request httpRequest) {
        try {
            Request request = createAndInitRequest("aggregate", httpRequest.body().asJson(), httpRequest);
            // Derive the acting userId from the auth token — never trust a client-supplied userId.
            String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
            request.getRequest().put(JsonKey.USER_ID, userId);
            validate(request);
            return actorResponseHandler(viewerAggregatorActor, request, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    private void validate(Request request) {
        String userId = (String) request.get(JsonKey.USER_ID);
        Object courseId = request.get(JsonKey.COURSE_ID);
        if (userId == null || userId.trim().isEmpty()
                || courseId == null || courseId.toString().trim().isEmpty()) {
            throw new ProjectCommonException(
                ResponseCode.mandatoryParamsMissing.getErrorCode(),
                "userId and courseId are mandatory",
                ResponseCode.CLIENT_ERROR.getResponseCode());
        }
    }
}
