package controllers.viewer;

import controllers.BaseController;
import org.apache.commons.lang3.StringUtils;
import org.apache.pekko.actor.ActorRef;
import org.sunbird.exception.ProjectCommonException;
import org.sunbird.keys.JsonKey;
import org.sunbird.message.ResponseCode;
import org.sunbird.request.Request;
import play.mvc.Http;
import play.mvc.Result;

import play.libs.Json;
import play.mvc.Results;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// view lifecycle APIs -> ViewConsumptionActor (view-consumption-actor)
public class ViewController extends BaseController {

    private final ActorRef viewConsumptionActor;

    @Inject
    public ViewController(@Named("view-consumption-actor") ActorRef viewConsumptionActor) {
        this.viewConsumptionActor = viewConsumptionActor;
    }

    public CompletionStage<Result> viewStart(Http.Request httpRequest) {
        return dispatch("viewStart", httpRequest);
    }

    public CompletionStage<Result> viewUpdate(Http.Request httpRequest) {
        return dispatch("viewUpdate", httpRequest);
    }

    public CompletionStage<Result> viewEnd(Http.Request httpRequest) {
        return dispatch("viewEnd", httpRequest);
    }

    public CompletionStage<Result> viewRead(Http.Request httpRequest) {
        return dispatch("viewRead", httpRequest);
    }

    public CompletionStage<Result> assessmentSubmit(Http.Request httpRequest) {
        return dispatch("viewAssess", httpRequest);
    }

    public CompletionStage<Result> assessmentRead(Http.Request httpRequest) {
        return dispatch("assessmentRead", httpRequest);
    }

    public Result health(Http.Request httpRequest) {
        ObjectNode json = Json.newObject();
        json.put("healthy", true);
        return Results.ok(json);
    }

    private CompletionStage<Result> dispatch(String operation, Http.Request httpRequest) {
        try {
            Request request = createAndInitRequest(operation, httpRequest.body().asJson(), httpRequest);
            // userId is derived from the auth token, never the client body
            String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
            request.getRequest().put(JsonKey.USER_ID, userId);
            // optional ?context=all (view.read) -> read all contents irrespective of context (type:"contextall")
            httpRequest.queryString("context").filter(StringUtils::isNotBlank)
                .ifPresent(ctx -> request.getRequest().put("context", ctx));
            validate(operation, request);
            return actorResponseHandler(viewConsumptionActor, request, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    // only contentId is mandatory; collectionId(courseId)/contextId(batchId) are optional and cascade in the actor (design scenarios 1-3)
    private void validate(String operation, Request request) {
        switch (operation) {
            case "viewStart": case "viewUpdate": case "viewEnd": case "viewAssess":
                requireNonBlank(request, "contentId");
                break;
            default: // viewRead / assessmentRead + others: userId is derived from the token; keys are optional
        }
    }

    private void requireNonBlank(Request request, String... keys) {
        for (String key : keys) {
            if (StringUtils.isBlank((String) request.getRequest().get(key))) {
                throw new ProjectCommonException(
                    ResponseCode.mandatoryParameterMissing.getErrorCode(),
                    ResponseCode.mandatoryParameterMissing.getErrorMessage() + " " + key,
                    ResponseCode.CLIENT_ERROR.getResponseCode());
            }
        }
    }
}
