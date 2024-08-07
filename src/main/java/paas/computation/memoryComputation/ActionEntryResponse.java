package paas.computation.memoryComputation;

/**
 * Created by chenzheng on 2021/1/31.
 */
public class ActionEntryResponse extends Response{
    private Object distributedDataset;

    public ActionEntryResponse(int taskStatus, Object distributedDataset, int errorCode, String errorMsg){
        super(taskStatus, errorCode, errorMsg);
        this.distributedDataset = distributedDataset;
    }

    public static ActionEntryResponse getResponse(ErrorCodeEnum errorCodeEnum, Object distributedDataset){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new ActionEntryResponse(
                    Response.TASK_STATUS_SUCCESS,
                    distributedDataset,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new ActionEntryResponse(
                Response.TASK_STATUS_FAILED,
                null,
                errorCodeEnum.getErrorCode(),
                errorCodeEnum.getErrorMsg()
        );
    }

    @Override
    public String toString() {
        return String.format("ActionEntryResponse {taskStatus=%s, distributedDataset=%s, errorCode=%s, errorMsg=%s}",
                this.getTaskStatus(), distributedDataset.toString(), this.getErrorCode(), this.getErrorMsg());
    }
}
