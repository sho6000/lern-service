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

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// summary APIs -> ViewerSummaryActor (viewer-summary-actor)
public class ViewSummaryController extends BaseController {

    private final ActorRef viewerSummaryActor;

    @Inject
    public ViewSummaryController(@Named("viewer-summary-actor") ActorRef viewerSummaryActor) {
        this.viewerSummaryActor = viewerSummaryActor;
    }

    public CompletionStage<Result> summaryRead(Http.Request httpRequest) {
        return dispatchBody("summaryRead", httpRequest);
    }

    public CompletionStage<Result> summaryList(String userId, Http.Request httpRequest) {
        try {
            Request request = createAndInitRequest("summaryList", httpRequest);
            requireOwnPath(request, userId);
            request.getRequest().put("userId", userId);
            return actorResponseHandler(viewerSummaryActor, request, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    public CompletionStage<Result> summaryDownload(String userId, Http.Request httpRequest) {
        try {
            Request request = createAndInitRequest("summaryDownload", httpRequest);
            requireOwnPath(request, userId);
            request.getRequest().put("userId", userId);
            String fmt = httpRequest.queryString("format").filter(StringUtils::isNotBlank).orElse("json");
            request.getRequest().put("format", fmt);
            return actorResponseHandler(viewerSummaryActor, request, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    public CompletionStage<Result> summaryDelete(String userId, Http.Request httpRequest) {
        try {
            Request request = httpRequest.body().asJson() != null
                ? createAndInitRequest("summaryDelete", httpRequest.body().asJson(), httpRequest)
                : createAndInitRequest("summaryDelete", httpRequest);
            requireOwnPath(request, userId);
            request.getRequest().put("userId", userId);
            return actorResponseHandler(viewerSummaryActor, request, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    private CompletionStage<Result> dispatchBody(String operation, Http.Request httpRequest) {
        try {
            Request request = createAndInitRequest(operation, httpRequest.body().asJson(), httpRequest);
            // userId from the auth token, never the body — matches ViewController.dispatch and blocks reading another user's summary
            String userId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
            request.getRequest().put(JsonKey.USER_ID, userId);
            return actorResponseHandler(viewerSummaryActor, request, timeout, null, httpRequest);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(createCommonExceptionResponse(e, httpRequest));
        }
    }

    // path userId must equal the authenticated user (IDOR guard)
    private void requireOwnPath(Request request, String pathUserId) {
        String authUserId = (String) request.getContext().getOrDefault(JsonKey.REQUESTED_FOR, request.getContext().get(JsonKey.REQUESTED_BY));
        if (authUserId == null || !authUserId.equals(pathUserId)) {
            throw new ProjectCommonException(
                ResponseCode.unAuthorized.getErrorCode(),
                ResponseCode.unAuthorized.getErrorMessage(),
                ResponseCode.UNAUTHORIZED.getResponseCode());
        }
    }
}
