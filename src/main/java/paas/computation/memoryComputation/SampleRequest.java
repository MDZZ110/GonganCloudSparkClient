package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SampleRequest extends BaseRequest implements Serializable {
    String distributedDataset;
    String replace;
    Float percentage;
    int randomSeed;

    public SampleRequest(String distributedDataset, String replace, Float percentage, int randomSeed) {
        this.distributedDataset = distributedDataset;
        this.replace = replace;
        this.percentage = percentage;
        this.randomSeed = randomSeed;
    }

}
