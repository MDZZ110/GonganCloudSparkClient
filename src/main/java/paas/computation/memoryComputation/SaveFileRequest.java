package paas.computation.memoryComputation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SaveFileRequest extends BaseRequest  implements Serializable {
    Object distributedDataset;
    String fileType;
    String filePath;

    public SaveFileRequest(String distributedDataset, String fileType, String filePath) {
        this.distributedDataset = distributedDataset;
        this.fileType = fileType;
        this.filePath = filePath;
    }

}
