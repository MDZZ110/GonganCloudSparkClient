package org.qingcloud;

import paas.computation.memoryComputation.*;

import java.awt.*;
import java.util.ArrayList;
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

        testActionEntry(token);

        testTransformationEntry(token);
    }

    public static void printFailed(String message) {
        System.out.println("==========> " + message  + " <==========");
    }

    public static Boolean testCollect(String token)  {
        CollectResponse resp = new MemoryComputationImpl().collect(Arrays.asList("1", "2", "3", "4", "5"), token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testCollect failed");
        return false;
    }

    public static Boolean testCount(String token)  {
        CountResponse resp = new MemoryComputationImpl().count(Arrays.asList("1", "2", "3", "4", "5"), token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testCount failed");
        return false;
    }

    public static Boolean testTake(String token)  {
        TakeResponse resp = new MemoryComputationImpl().take(Arrays.asList("1", "2", "3", "4", "5"), 3, token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testTake failed");
        return false;
    }

    public static Boolean testSaveFile(String token)  {
        List<String> fileTypeList = Arrays.asList("txt", "csv", "json", "parquet", "orc");
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

        return pass;
    }

    public static Boolean testMap(String token) {
        String userDefinedFunction = "com.qingcloud.MapObject.udfMap";
        MapResponse resp = new MemoryComputationImpl().map(Arrays.asList("1", "2", "3", "4", "5"), userDefinedFunction, token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testMap failed");
        return false;
    }

    public static Boolean testFilter(String token) {
        String userDefinedFunction = "com.qingcloud.MapObject.udfFilter";
        FilterResponse resp = new MemoryComputationImpl().filter(Arrays.asList("1", "2", "3", "4", "5"), userDefinedFunction, token);
        if (resp.getErrorCode() == 0) {
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
            return true;
        }

        printFailed("testIntersection failed");
        return false;
    }

    public static Boolean testDistinct(String token) {
        DistinctResponse resp = new MemoryComputationImpl().distinct(Arrays.asList("1", "1", "3", "3", "5"), token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testDistinct failed");
        return false;
    }

    public static Boolean testGroupByKey(String token) {
        GroupByKeyResponse resp = new MemoryComputationImpl().groupByKey(Arrays.asList("A,1", "B,2", "A,3", "C,4"), token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testGroupByKey failed");
        return false;

    }

    public static Boolean testReduceByKey(String token) {
        ReduceByKeyResponse resp = new MemoryComputationImpl().reduceByKey(Arrays.asList("A,1", "B,2", "A,3", "C,4"), "udf.Udf.udfReduce", "A", token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testReduceByKey failed");
        return false;
    }

    public static Boolean testSortByKey(String token) {
        SortByKeyResponse resp = new MemoryComputationImpl().sortByKey(Arrays.asList("A,1", "B,2", "A,3", "C,4"), "0", "A", token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testSortByKey failed");
        return false;
    }

    public static Boolean testJoin(String token) {
        JoinResponse resp = new MemoryComputationImpl().join(Arrays.asList("A,1", "B,2", "A,3", "C,4"), Arrays.asList("A,1", "B,2", "A,3", "C,4"), 0, token);
        if (resp.getErrorCode() == 0) {
            return true;
        }

        printFailed("testJoin failed");
        return false;
    }

    public static Boolean testPartition(String token) {
        PartitionResponse resp = new MemoryComputationImpl().partition(Arrays.asList("A,1", "B,2", "A,3", "B,4"), 2, token);
        if (resp.getErrorCode() == 0) {
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

        resp = new MemoryComputationImpl().actionEntry(Arrays.asList("1", "2", "3", "4", "5"), 2, "", token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Count failed");
            return false;
        }

        String takeParameter = "{\"amount\":3}";
        resp = new MemoryComputationImpl().actionEntry(Arrays.asList("1", "2", "3", "4", "5"), 3, takeParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testActionEntry Take failed");
            return false;
        }

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

            String remoteSaveFileParameter = String.format("{\"fileType\": \"%s\", \"filePath\": \"%s\"}", fileTypeName, remoteFilePath);;
            resp = new MemoryComputationImpl().actionEntry(dataset, 4, remoteSaveFileParameter, token);
            if (resp.getErrorCode() != 0) {
                printFailed("testActionEntry saveFile local failed");
                return false;
            }
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

        HashMap<String, List<String>> filterFuncDataMap = new HashMap<>();
        filterFuncDataMap.put("distributedDataset", Arrays.asList("1", "2", "3", "4", "5"));
        String filterParameter = "{\"userDefinedFunction\":\"com.qingcloud.MapObject.udfFilter\"}";
        resp = new MemoryComputationImpl().transformationEntry(filterParameter, "filter", filterParameter, token);
        if (resp.getErrorCode() != 0) {
            printFailed("testTransformationEntry Filter failed");
            return false;
        }

        return true;
    }

}
