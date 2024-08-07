package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SubmitRequest extends BaseRequest implements Serializable {
    String distributedDataset;

    public SubmitRequest(String distributedDataset) {
        this.distributedDataset = distributedDataset;
    }
}
