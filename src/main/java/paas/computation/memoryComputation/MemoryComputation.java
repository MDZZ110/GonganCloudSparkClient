package paas.computation.memoryComputation;

/**
 * Created by chenzheng on 2021/1/31.
 */
public interface MemoryComputation {
    public ActionEntryResponse actionEntry(Object distributedDataset,
                                           int function,
                                           String parameterList,
                                           String accessToken);

    public TransformatEntryResponse transformationEntry(Object distributedDataset,
                                                        String function,
                                                        String parameterList,
                                                        String accessToken);

    public CollectResponse collect(Object distributedDataset, String accessToken);

    public CountResponse count(Object distributedDataset, String accessToken);

    public TakeResponse take(Object distributedDataset, int amount, String accessToken);

    public Response saveFile(Object distributedDataset, String fileType, String filePath, String accessToken);

    public MapResponse map(Object distributedDataset, String userDefinedFunction, String accessToken);

    public FilterResponse filter(Object distributedDataset, String userDefinedFunction, String accessToken);

    public SampleResponse sample(Object distributedDataset, String replace, Double percentage, Long randomSeed, String accessToken);

    public UnionResponse union(Object distributedDataset1, Object distributedDataset2, String accessToken);

    public IntersectionResponse intersection(Object distributedDataset1, Object distributedDataset2, String accessToken);

    public DistinctResponse distinct(Object distributedDataset, String accessToken);

    public GroupByKeyResponse groupByKey(Object distributedDataset, String accessToken);

    public ReduceByKeyResponse reduceByKey(Object distributedDataset, String reduceFunction, String keyField, String accessToken);

    public SortByKeyResponse sortByKey(Object distributedDataset, String sort, String keyField, String accessToken);

    public JoinResponse join(Object distributedDataset1, Object distributedDataset2, int joinMethod, String accessToken);

    public PartitionResponse partition(Object distributedDataset, int partitionNumber, String accessToken);

}
