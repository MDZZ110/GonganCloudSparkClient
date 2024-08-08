package paas.common.response;


import com.fasterxml.jackson.annotation.JsonProperty;
import paas.computation.memoryComputation.ErrorCodeEnum;

/**
 * Created by chenzheng on 2021/2/1.
 */
public class Response {
    public static final int TASK_STATUS_FAILED = 0;
    public static final int TASK_STATUS_SUCCESS = 1;

    @JsonProperty("taskStatus")
    private int taskStatus;

    @JsonProperty("errorCode")
    private int errorCode;

    @JsonProperty("errorMsg")
    private String errorMsg;

    public Response() {}

    public Response(int taskStatus, int errorCode, String errorMsg){
        this.taskStatus = taskStatus;
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }

    public int getTaskStatus() {
        return taskStatus;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public static Response getResponse(ErrorCodeEnum errorCodeEnum){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new Response(
                    Response.TASK_STATUS_SUCCESS,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new Response(
                Response.TASK_STATUS_FAILED,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }

    @Override
    public String toString() {
        return "Response{" +
                "taskStatus=" + taskStatus +
                ", errorCode=" + errorCode +
                ", errorMsg='" + errorMsg + '\'' +
                '}';
    }

}
