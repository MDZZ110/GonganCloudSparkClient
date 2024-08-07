package paas.computation.memoryComputation;

/**
 * Created by chenzheng on 2021/2/2.
 */
public enum ErrorCodeEnum {
    SUCCESS(0, ""),
    FAILED(1, "failed"),
    DATA_TYPE_NOT_SUPPORTED(2, "data type not supported"),
    PERMISSION_DENIED(3, "permission denied"),
    FILE_TYPE_NOT_SUPPORTED(4, "file type not supported"),
    USER_DEFINED_FUNCTION_PATH_NOT_VALID(5, "user defined function path not valid"),
    INVALID_INPUT_PARAMS_LENGTH(6, "invalid input params length"),
    PARAM_OUT_OF_RANGE(7, "param out of range"),
    PARAM_NOT_FOUND(8, "not enough params found"),
    FUNCTION_NOT_SUPPORTED(9, "function not supported");


    private final int errorCode;
    private final String errorMsg;

    ErrorCodeEnum(int errorCode, String errorMsg){
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }
}
