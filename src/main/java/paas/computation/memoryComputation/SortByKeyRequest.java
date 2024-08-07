package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SortByKeyRequest extends BaseRequest  implements Serializable  {
    String distributedDataset;
    String sort;
    String keyField;

    public SortByKeyRequest(String distributedDataset, String sort, String keyField) {
        this.distributedDataset = distributedDataset;
        this.sort = sort;
        this.keyField = keyField;
    }
}
