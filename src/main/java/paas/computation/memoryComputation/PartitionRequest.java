package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class PartitionRequest extends BaseRequest  implements Serializable {
    String distributedDataset;
    int partitionNumber;

    public PartitionRequest(String distributedDataset, int partitionNumber) {
        this.distributedDataset = distributedDataset;
        this.partitionNumber = partitionNumber;
    }
}
