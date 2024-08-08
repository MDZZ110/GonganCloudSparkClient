package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import paas.common.response.Response;

import java.io.IOException;

/**
 * Created by chenzheng on 2021/2/5.
 */
public class CountResponse extends Response {
    @JsonProperty("totalNum")
    private int totalNum;

    public CountResponse() {}

    public CountResponse(int taskStatus, int totalNum, int errorCode, String errorMsg){
        super(taskStatus, errorCode, errorMsg);
        this.totalNum = totalNum;
    }

    public static CountResponse convertJsonToResponse(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return  mapper.readValue(json, CountResponse.class);
    }


    public static CountResponse getResponse(ErrorCodeEnum errorCodeEnum, int totalNum){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new CountResponse(
                    Response.TASK_STATUS_SUCCESS,
                    totalNum,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new CountResponse(
                Response.TASK_STATUS_FAILED,
                0,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }

    public int getTotalNum() {
        return totalNum;
    }

    @Override
    public String toString() {
        return String.format("CountResponse{taskStatus=%s, totalNum=%s, errorCode=%s, errorMsg=%s}",
                this.getTaskStatus(), totalNum, this.getErrorCode(), this.getErrorMsg());
    }
}
