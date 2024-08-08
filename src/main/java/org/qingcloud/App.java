package org.qingcloud;

import paas.computation.memoryComputation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        String accessKey = "KRQYRAWSUPWQUYEYEFCM";
        String secretKey = "v0kvaZC979PPHG1HxlHivtFEyzj5rQy6usP2e6mB";
        String token = accessKey + "|" + secretKey;

        testCollect(token);

        testCount(token);

        testTake(token);

        // TODO ORC 依赖包有问题
        testSaveFile(token);

        testMap(token);

        testFilter(token);

        testSample(token);

        testUnion(token);

        testIntersection(token);

        testDistinct(token);

        testGroupByKey(token);

        testReduceByKey(token);

        testSortByKey(token);

        testJoin(token);

        testPartition(token);

        // TODO 同 ORC 依赖包的问题
        testActionEntry(token);


        testTransformationEntry(token);
    }

    public static void printFailed(String message) {
        System.out.println("==========> " + message  + " <==========");
    }

    public static Boolean testCollect(String token)  {
        CollectResponse resp = new MemoryComputationImpl().collect(Arrays.asList("1", "2", "3", "4", "5"), token);
        if (resp.getErrorCode() == 0) {
            printFailed("testCollect Successfully!");
            return true;
        }

        printFailed("testCollect failed");
        return false;
    }

    public static Boolean testCount(String token)  {
        CountResponse resp = new MemoryComputationImpl().count(Arrays.asList("1", "2", "3", "4", "5"), token);
        if (resp.getErrorCode() == 0) {
            printFailed("testCount Successfully!");
            return true;
        }

        printFailed("testCount failed");
        return false;
    }

    public static Boolean testTake(String token)  {
        TakeResponse resp = new MemoryComputationImpl().take(Arrays.asList("1", "2", "3", "4", "5"), 3, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testTake Successfully!");
            return true;
        }

        printFailed("testTake failed");
        return false;
    }

    public static Boolean testSaveFile(String token)  {
//        List<String> fileTypeList = Arrays.asList("txt", "csv", "json", "parquet", "orc");
        List<String> fileTypeList = Arrays.asList("orc");
        String localFileDir = "file:///tmp/memcompute";
        String remoteFileDir = "dfs:///hadoop/memcompute";
        SaveFileResponse resp;
        Object dataset = Arrays.asList("Andy,32", "Ben,41");
        Boolean pass = true;
        for(String fileTypeName:fileTypeList){
            String localFilePath = localFileDir + "/test_save_file_" + fileTypeName;
            String remoteFilePath = remoteFileDir + "/test_save_file_" + fileTypeName;

            resp = new MemoryComputationImpl().saveFile(dataset, fileTypeName, localFilePath, token);
            if (resp.getErrorCode() != 0) {
                pass = false;
                break;
            }

            resp = new MemoryComputationImpl().saveFile(dataset, fileTypeName, remoteFilePath, token);
            if (resp.getErrorCode() != 0) {
                pass = false;
                break;
            }
        }

        if (!pass) {
            printFailed("testSaveFile failed");
        }

        printFailed("testSaveFile Successfully!");
        return pass;
    }

    public static Boolean testMap(String token) {
        String userDefinedFunction = "com.qingcloud.MapObject.udfMap";
        MapResponse resp = new MemoryComputationImpl().map(Arrays.asList("1", "2", "3", "4", "5"), userDefinedFunction, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testMap Successfully");
            return true;
        }

        printFailed("testMap failed");
        return false;
    }

    public static Boolean testFilter(String token) {
        String userDefinedFunction = "com.qingcloud.MapObject.udfFilter";
        FilterResponse resp = new MemoryComputationImpl().filter(Arrays.asList("1", "2", "3", "4", "5"), userDefinedFunction, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testFilter Successfully");
            return true;
        }

        printFailed("testFilter failed");
        return false;
    }

    public static Boolean testSample(String token) {
        String replace = "1";
        Double percentage = 0.32;
        Long randomSeed = 100023L;
        SampleResponse resp = new MemoryComputationImpl().sample(Arrays.asList("1", "2", "3", "4", "5"), replace, percentage, randomSeed, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testSample Successfully");
            return true;
        }

        printFailed("testSample failed");
        return false;
    }

    public static Boolean testUnion(String token) {
        List<String> dataset1 = Arrays.asList("1", "2", "3", "4", "5");
        List<String> dataset2 = Arrays.asList("6", "7", "8", "9", "10");

        UnionResponse resp = new MemoryComputationImpl().union(dataset1, dataset2, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testUnion Successfully");
            return true;
        }

        printFailed("testUnion failed");
        return false;
    }

    public static Boolean testIntersection(String token) {
        List<String> dataset1 = Arrays.asList("1", "2", "3", "4", "5");
        List<String> dataset2 = Arrays.asList("4", "5", "6", "7", "8");

        IntersectionResponse resp = new MemoryComputationImpl().intersection(dataset1, dataset2, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testIntersection Successfully");
            return true;
        }

        printFailed("testIntersection failed");
        return false;
    }

    public static Boolean testDistinct(String token) {
        DistinctResponse resp = new MemoryComputationImpl().distinct(Arrays.asList("1", "1", "3", "3", "5"), token);
        if (resp.getErrorCode() == 0) {
            printFailed("testDistinct Successfully");
            return true;
        }

        printFailed("testDistinct failed");
        return false;
    }

    public static Boolean testGroupByKey(String token) {
        GroupByKeyResponse resp = new MemoryComputationImpl().groupByKey(Arrays.asList("A,1", "B,2", "A,3", "C,4"), token);
        if (resp.getErrorCode() == 0) {
            printFailed("testGroupByKey Successfully");
            return true;
        }

        printFailed("testGroupByKey failed");
        return false;

    }

    public static Boolean testReduceByKey(String token) {
        ReduceByKeyResponse resp = new MemoryComputationImpl().reduceByKey(Arrays.asList("A,1", "B,2", "A,3", "C,4"), "udf.Udf.udfReduce", "A", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testReduceByKey Successfully");
            return true;
        }

        printFailed("testReduceByKey failed");
        return false;
    }

    public static Boolean testSortByKey(String token) {
        SortByKeyResponse resp = new MemoryComputationImpl().sortByKey(Arrays.asList("A,1", "B,2", "A,3", "C,4"), "0", "A", token);
        if (resp.getErrorCode() == 0) {
            printFailed("testSortByKey Successfully");
            return true;
        }

        printFailed("testSortByKey failed");
        return false;
    }

    public static Boolean testJoin(String token) {
        JoinResponse resp = new MemoryComputationImpl().join(Arrays.asList("A,1", "B,2", "K,3", "D,4"), Arrays.asList("A,2", "B,2", "A,5", "C,6"), 0, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testJoin Successfully");
            return true;
        }

        printFailed("testJoin failed");
        return false;
    }

    public static Boolean testPartition(String token) {
        PartitionResponse resp = new MemoryComputationImpl().partition(Arrays.asList("A,1", "B,2", "A,3", "B,4"), 2, token);
        if (resp.getErrorCode() == 0) {
            printFailed("testPartition Successfully");
            return true;
        }

        printFailed("testPartition failed");
        return false;
    }

    public static Boolean testActionEntry(String token) {
        ActionEntryResponse resp = new MemoryComputationImpl().actionEntry(Arrays.asList("1", "2", "3", "4", "5"), 1, "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Collect failed");
            return false;
        }

        printFailed("testActionEntry Collect Successfully!");

        resp = new MemoryComputationImpl().actionEntry(Arrays.asList("1", "2", "3", "4", "5"), 2, "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Count failed");
            return false;
        }

        printFailed("testActionEntry Count Successfully!");

        String takeParameter = "{\"amount\":3}";
        resp = new MemoryComputationImpl().actionEntry(Arrays.asList("1", "2", "3", "4", "5"), 3, takeParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Take failed");
            return false;
        }

        printFailed("testActionEntry Take Successfully!");

        List<String> fileTypeList = Arrays.asList("txt", "csv", "json", "parquet", "orc");
        String localFileDir = "file:///tmp/actionEntry";
        String remoteFileDir = "dfs:///hadoop/actionEntry";
        Object dataset = Arrays.asList("Andy,32", "Ben,41");
        for(String fileTypeName:fileTypeList){
            String localFilePath = localFileDir + "/test_save_file_" + fileTypeName;
            String remoteFilePath = remoteFileDir + "/test_save_file_" + fileTypeName;

            String localSaveFileParameter = String.format("{\"fileType\": \"%s\", \"filePath\": \"%s\"}", fileTypeName, localFilePath);
            resp = new MemoryComputationImpl().actionEntry(dataset, 4, localSaveFileParameter, token);
            if (resp.getErrorCode() != 0) {
                printFailed("testActionEntry saveFile local failed");
                return false;
            }

            printFailed("testActionEntry saveFile " + fileTypeName + " to local Successfully");

            String remoteSaveFileParameter = String.format("{\"fileType\": \"%s\", \"filePath\": \"%s\"}", fileTypeName, remoteFilePath);;
            resp = new MemoryComputationImpl().actionEntry(dataset, 4, remoteSaveFileParameter, token);
            if (resp.getErrorCode() != 0) {
                printFailed("testActionEntry saveFile local failed");
                return false;
            }

            printFailed("testActionEntry saveFile " + fileTypeName + " to remote HDFS Successfully");
        }

        return true;

    }

    public static Boolean testTransformationEntry(String token) {
        HashMap<String, List<String>> mapFuncDataMap = new HashMap<>();
        mapFuncDataMap.put("distributedDataset", Arrays.asList("1", "2", "3", "4", "5"));
        String mapParameter = "{\"userDefinedFunction\":\"com.qingcloud.MapObject.udfMap\"}";
        TransformatEntryResponse resp = new MemoryComputationImpl().transformationEntry(mapFuncDataMap, "map", mapParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Map failed");
            return false;
        }

        printFailed("testTransformationEntry Map Successfully!");

        HashMap<String, List<String>> filterFuncDataMap = new HashMap<>();
        filterFuncDataMap.put("distributedDataset", Arrays.asList("1", "2", "3", "4", "5"));
        String filterParameter = "{\"userDefinedFunction\":\"com.qingcloud.MapObject.udfFilter\"}";
        resp = new MemoryComputationImpl().transformationEntry(filterFuncDataMap, "filter", filterParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Filter failed");
            return false;
        }
        printFailed("testTransformationEntry Filter Successfully");

        String replace = "1";
        Double percentage = 0.32;
        Long randomSeed = 100023L;
        HashMap<String, Object> sampleFuncDataMap = new HashMap<>();
        sampleFuncDataMap.put("distributedDataset", Arrays.asList("1", "2", "3", "4", "5"));
        String sampleParameter = String.format("{\"replace\":\"%s\", \"percentage\":\"%s\", \"randomSeed\": \"%s\"}", replace, percentage, randomSeed);
        resp = new MemoryComputationImpl().transformationEntry(sampleFuncDataMap, "sample", sampleParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Sample failed");
            return false;
        }
        printFailed("testTransformationEntry Sample Successfully");

        List<String> unionDataset1 = Arrays.asList("1", "2", "3", "4", "5");
        List<String> unionDataset2 = Arrays.asList("6", "7", "8", "4", "5");
        HashMap<String, Object> unionFuncDataMap = new HashMap<>();
        unionFuncDataMap.put("distributedDataset1", unionDataset1);
        unionFuncDataMap.put("distributedDataset2", unionDataset2);
        String unionParameter = "{\"distributedDataset1\": \"distributedDataset1\", \"distributedDataset2\": \"distributedDataset2\"}";
        resp = new MemoryComputationImpl().transformationEntry(unionFuncDataMap, "union", unionParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry union failed");
            return false;
        }
        printFailed("testTransformationEntry union Successfully");

        List<String> intersectionDataset1 = Arrays.asList("1", "2", "3", "4", "6");
        List<String> intersectionDataset2 = Arrays.asList("6", "7", "8", "4", "9");
        HashMap<String, Object> intersectionFuncDataMap = new HashMap<>();
        intersectionFuncDataMap.put("distributedDataset1", intersectionDataset1);
        intersectionFuncDataMap.put("distributedDataset2", intersectionDataset2);
        String intersectionParameter = "{\"distributedDataset1\": \"distributedDataset1\", \"distributedDataset2\": \"distributedDataset2\"}";
        resp = new MemoryComputationImpl().transformationEntry(intersectionFuncDataMap, "intersection", intersectionParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry intersection failed");
            return false;
        }
        printFailed("testTransformationEntry intersection Successfully");

        List<String> distinctDataset = Arrays.asList("1", "1", "2", "2", "3");
        HashMap<String, Object> distinctFuncDataMap = new HashMap<>();
        distinctFuncDataMap.put("distributedDataset", distinctDataset);
        resp = new MemoryComputationImpl().transformationEntry(distinctFuncDataMap, "distinct", "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry distinct failed");
            return false;
        }
        printFailed("testTransformationEntry distinct Successfully");

        List<String> groupDataset = Arrays.asList("A,1", "B,2", "A,3", "C,4");
        HashMap<String, Object> groupFuncDataMap = new HashMap<>();
        groupFuncDataMap.put("distributedDataset", groupDataset);
        resp = new MemoryComputationImpl().transformationEntry(groupFuncDataMap, "group", "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry group failed");
            return false;
        }
        printFailed("testTransformationEntry group Successfully");

        List<String> reduceDataset = Arrays.asList("A,1", "B,2", "A,5", "C,4");
        HashMap<String, Object> reduceFuncDataMap = new HashMap<>();
        reduceFuncDataMap.put("distributedDataset", reduceDataset);
        String reduceParameter = "{\"reduceFunction\":\"udf.Udf.udfReduce\", \"keyField\":\"A\"}";
        resp = new MemoryComputationImpl().transformationEntry(reduceFuncDataMap, "reduce", reduceParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry reduce failed");
            return false;
        }
        printFailed("testTransformationEntry reduce Successfully");

        List<String> sortDataset = Arrays.asList("A,1", "B,2", "A,3", "C,4");
        String sortParameter = "{\"sort\":\"0\", \"keyField\":\"A\"}";
        HashMap<String, Object> sortFuncDataMap = new HashMap<>();
        sortFuncDataMap.put("distributedDataset", sortDataset);
        resp = new MemoryComputationImpl().transformationEntry(sortFuncDataMap, "sort", sortParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry sort failed");
            return false;
        }
        printFailed("testTransformationEntry sort Successfully");

        List<String> joinDataset1 = Arrays.asList("A,1", "B,2", "K,3", "D,4");
        List<String> joinDataset2 = Arrays.asList("A,2", "B,2", "A,5", "C,6");
        HashMap<String, Object> joinFuncDataMap = new HashMap<>();
        joinFuncDataMap.put("distributedDataset1", joinDataset1);
        joinFuncDataMap.put("distributedDataset2", joinDataset2);
        String joinParameter = "{\"joinMethod\":\"1\", \"distributedDataset1\": \"distributedDataset1\", \"distributedDataset2\": \"distributedDataset2\"}";
        resp = new MemoryComputationImpl().transformationEntry(joinFuncDataMap, "join", joinParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry join failed");
            return false;
        }
        printFailed("testTransformationEntry join Successfully");

        List<String> partitionDataset = Arrays.asList("A,1", "B,2", "A,3", "B,4");
        HashMap<String, Object> partitionFuncDataMap = new HashMap<>();
        partitionFuncDataMap.put("distributedDataset", partitionDataset);
        String partitionParameter = "{\"partitionNumber\":\"2\"}";
        resp = new MemoryComputationImpl().transformationEntry(partitionFuncDataMap, "partition", partitionParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry partition failed");
            return false;
        }
        printFailed("testTransformationEntry partition Successfully");


        return true;
    }

}
