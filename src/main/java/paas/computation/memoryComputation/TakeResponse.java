package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import paas.common.response.Response;

import java.io.IOException;
import java.util.List;

public class TakeResponse extends Response {
    @JsonProperty("distributedDataset")
    private String distributedDataset;

    public TakeResponse() {}

    public TakeResponse(int taskStatus, String distributedDataset, int errorCode, String errorMsg){
        super(taskStatus, errorCode, errorMsg);
        this.distributedDataset = distributedDataset;
    }

    public static TakeResponse convertJsonToResponse(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return  mapper.readValue(json, TakeResponse.class);
    }

    public static TakeResponse getResponse(ErrorCodeEnum errorCodeEnum, String distributedDataset){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new TakeResponse(
                    Response.TASK_STATUS_SUCCESS,
                    distributedDataset,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new TakeResponse(
                Response.TASK_STATUS_FAILED,
                null,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }

    public String getDistributedDataset() {
        return distributedDataset;
    }

    @Override
    public String toString() {
        return String.format("TakeResponse{taskStatus=%s, distributedDataset=%s, errorCode=%s, errorMsg=%s}",
                this.getTaskStatus(), distributedDataset.toString(), this.getErrorCode(), this.getErrorMsg());
    }

}
