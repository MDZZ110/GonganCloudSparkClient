package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class JoinRequest extends BaseRequest implements Serializable  {
    String distributedDataset1;
    String distributedDataset2;
    int joinMethod;

    public JoinRequest(String distributedDataset1, String distributedDataset2, int joinMethod) {
        this.distributedDataset1 = distributedDataset1;
        this.distributedDataset2 = distributedDataset2;
        this.joinMethod = joinMethod;
    }
}
