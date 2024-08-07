package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class IntersectionRequest extends BaseRequest implements Serializable {
    String distributedDataset1;
    String distributedDataset2;

    public IntersectionRequest(String distributedDataset1, String distributedDataset2) {
        this.distributedDataset1 = distributedDataset1;
        this.distributedDataset2 = distributedDataset2;
    }
}
