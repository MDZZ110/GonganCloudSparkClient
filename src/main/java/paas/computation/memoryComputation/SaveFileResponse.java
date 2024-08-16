package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import paas.common.response.Response;

import java.io.IOException;
import java.util.List;

public class SaveFileResponse extends Response {
    @JsonProperty("distributedDataset")
    private List<?> distributedDataset;

    public SaveFileResponse() {}

    public SaveFileResponse(int taskStatus, int errorCode, String errorMsg) {
        super(taskStatus, errorCode, errorMsg);
    }

    public List<?> getDistributedDataset() {
        return distributedDataset;
    }

    public static SaveFileResponse convertJsonToResponse(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return  mapper.readValue(json, SaveFileResponse.class);
    }

    public static SaveFileResponse getResponse(ErrorCodeEnum errorCodeEnum){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new SaveFileResponse(
                    Response.TASK_STATUS_SUCCESS,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new SaveFileResponse(
                Response.TASK_STATUS_FAILED,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }
}
