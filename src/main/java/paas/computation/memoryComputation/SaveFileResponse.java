package paas.computation.memoryComputation;

import com.fasterxml.jackson.databind.ObjectMapper;
import paas.common.response.Response;

import java.io.IOException;

public class SaveFileResponse extends Response {
    public SaveFileResponse() {}

    public SaveFileResponse(int taskStatus, int errorCode, String errorMsg) {
        super(taskStatus, errorCode, errorMsg);
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
