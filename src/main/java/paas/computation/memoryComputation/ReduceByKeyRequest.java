package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class ReduceByKeyRequest extends BaseRequest  implements Serializable {
    String distributedDataset;
    String reduceFunction;
    String keyField;

    public ReduceByKeyRequest(String distributedDataset, String reduceFunction, String keyField) {
        this.distributedDataset = distributedDataset;
        this.reduceFunction = reduceFunction;
        this.keyField = keyField;
    }

}
