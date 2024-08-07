package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class MapRequest extends BaseRequest implements Serializable {
    String distributedDataset;
    String userDefinedFunction;

    public MapRequest(String distributedDataset, String userDefinedFunction) {
        this.distributedDataset = distributedDataset;
        this.userDefinedFunction = userDefinedFunction;
    }
}
