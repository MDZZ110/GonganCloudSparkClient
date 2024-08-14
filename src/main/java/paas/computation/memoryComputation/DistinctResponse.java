package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import paas.common.response.Response;

import java.io.IOException;

public class DistinctResponse extends Response {
    @JsonProperty("distributedDataset")
    private String distributedDataset;

    public DistinctResponse() {}

    public DistinctResponse(int taskStatus, String distributedDataset, int errorCode, String errorMsg){
        super(taskStatus, errorCode, errorMsg);
        this.distributedDataset = distributedDataset;
    }

    public static DistinctResponse convertJsonToResponse(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return  mapper.readValue(json, DistinctResponse.class);
    }

    public static DistinctResponse getResponse(ErrorCodeEnum errorCodeEnum, String distributedDataset){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new DistinctResponse(
                    TASK_STATUS_SUCCESS,
                    distributedDataset,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new DistinctResponse(
                TASK_STATUS_FAILED,
                null,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }

    public Object getDistributedDataset() {
        return distributedDataset;
    }

    @Override
    public String toString() {
        return String.format("DistinctResponse{taskStatus=%s, distributedDataset=%s, errorCode=%s, errorMsg=%s}",
                this.getTaskStatus(), distributedDataset, this.getErrorCode(), this.getErrorMsg());
    }
}
