package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class UnionRequest extends BaseRequest implements Serializable {
    String distributedDataset1;
    String distributedDataset2;

    public UnionRequest(String distributedDataset1, String distributedDataset2) {
        this.distributedDataset1 = distributedDataset1;
        this.distributedDataset2 = distributedDataset2;
    }
}
