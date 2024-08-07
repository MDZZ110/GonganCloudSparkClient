package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class TakeRequest extends BaseRequest implements Serializable  {
    String distributedDataset;
    int amount;

    public TakeRequest(String distributedDataset, int amount) {
        this.distributedDataset = distributedDataset;
        this.amount = amount;
    }
}
