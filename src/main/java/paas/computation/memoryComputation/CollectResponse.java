package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import paas.common.response.Response;

import java.io.IOException;
import java.util.List;

/**
 * Created by chenzheng on 2021/2/6.
 */
public class CollectResponse extends Response {
    @JsonProperty("result")
    private List<Integer> result;

    public CollectResponse() {}

    public CollectResponse(int taskStatus, List<Integer> result, int errorCode, String errorMsg){
        super(taskStatus, errorCode, errorMsg);
        this.result = result;
    }

    public static CollectResponse convertJsonToResponse(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return  mapper.readValue(json, CollectResponse.class);
    }

    public static CollectResponse getResponse(ErrorCodeEnum errorCodeEnum, List<Integer> result){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new CollectResponse(
                    Response.TASK_STATUS_SUCCESS,
                    result,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new CollectResponse(
                Response.TASK_STATUS_FAILED,
                null,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }

    public List<Integer> getResult() {
        return result;
    }

    @Override
    public String toString() {
        return String.format("CollectResponse{taskStatus=%s, result=%s, errorCode=%s, errorMsg=%s}",
                this.getTaskStatus(), result, this.getErrorCode(), this.getErrorMsg());
    }
}
