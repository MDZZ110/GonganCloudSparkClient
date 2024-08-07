package paas.computation.memoryComputation;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class PartitionResponse extends Response {
    private Object distributedDataset;

    public PartitionResponse(int taskStatus, Object distributedDataset, int errorCode, String errorMsg){
        super(taskStatus, errorCode, errorMsg);
        this.distributedDataset = distributedDataset;
    }

    public static PartitionResponse convertJsonToResponse(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return  mapper.readValue(json, PartitionResponse.class);
    }

    public static PartitionResponse getResponse(ErrorCodeEnum errorCodeEnum, Object distributedDataset){
        if(errorCodeEnum == ErrorCodeEnum.SUCCESS){
            return new PartitionResponse(
                    Response.TASK_STATUS_SUCCESS,
                    distributedDataset,
                    ErrorCodeEnum.SUCCESS.getErrorCode(),
                    ErrorCodeEnum.SUCCESS.getErrorMsg()
            );
        }

        return new PartitionResponse(
                Response.TASK_STATUS_FAILED,
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
        return String.format("PartitionResponse{taskStatus=%s, distributedDataset=%s, errorCode=%s, errorMsg=%s}",
                this.getTaskStatus(), distributedDataset.toString(), this.getErrorCode(), this.getErrorMsg());
    }
}
