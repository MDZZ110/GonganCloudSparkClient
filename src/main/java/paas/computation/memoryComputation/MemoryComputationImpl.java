package paas.computation.memoryComputation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import paas.common.response.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Created by chenzheng on 2021/2/1.
 */
public class MemoryComputationImpl implements MemoryComputation{
    public String sendRequest(String apiPath, String contentJson) throws IOException {
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(300, TimeUnit.SECONDS)
                .readTimeout(300, TimeUnit.SECONDS)
                .writeTimeout(300, TimeUnit.SECONDS)
                .build();
        MediaType typeJson = MediaType.parse("application/json; charset=utf-8");
        String url = CommonUtil.REMOTE_SERVER + apiPath;
        RequestBody body = RequestBody.create(typeJson, contentJson);
        Request request = new Request.Builder().url(url).post(body).build();
        okhttp3.Response response = httpClient.newCall(request).execute();
        return response.body().string();
    }

    @Override
    public ActionEntryResponse actionEntry(
            Object distributedDataset,
            int function,
            String parameterList,
            String accessToken){

        final int ACTION_METHOD_COLLECT = 1;
        final int ACTION_METHOD_COUNT = 2;
        final int ACTION_METHOD_TAKE = 3;
        final int ACTION_METHOD_SAVE_FILE = 4;

        HashSet<Integer> methodTypeSet = new HashSet<>(Arrays.asList(
                ACTION_METHOD_COLLECT, ACTION_METHOD_COUNT, ACTION_METHOD_TAKE, ACTION_METHOD_SAVE_FILE
        ));

        if(!methodTypeSet.contains(function)){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PARAM_OUT_OF_RANGE, null);
        }

        if(!CommonUtil.valiateStringType(parameterList, 0, 1000)){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        if(!CommonUtil.validateUserIdentity(accessToken)){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, "");
        }

        ActionEntryResponse result = null;

        switch (function){
            case ACTION_METHOD_COLLECT:
                result = this.collect(distributedDataset, parameterList, accessToken);
                break;
            case ACTION_METHOD_COUNT:
                result = this.count(distributedDataset, parameterList, accessToken);
                break;
            case ACTION_METHOD_TAKE:
                result = this.take(distributedDataset, parameterList, accessToken);
                break;
            case ACTION_METHOD_SAVE_FILE:
                result = this.saveFile(distributedDataset, parameterList, accessToken);
                break;
        }

        return result;
    }

    @Override
    public TransformatEntryResponse transformationEntry(
            Object distributedDataset,
            String function,
            String parameterList,
            String accessToken){


        final String TRANS_METHOD_MAP = "map";
        final String TRANS_METHOD_FILTER = "filter";
        final String TRANS_METHOD_SAMPLE = "sample";
        final String TRANS_METHOD_UNION = "union";
        final String TRANS_METHOD_INTERSECTION = "intersection";
        final String TRANS_METHOD_DISTINCT = "distinct";
        final String TRANS_METHOD_GROUP = "group";
        final String TRANS_METHOD_REDUCE = "reduce";
        final String TRANS_METHOD_SORT = "sort";
        final String TRANS_METHOD_JOIN = "join";
        final String TRANS_METHOD_PARTITION = "partition";

        HashSet<String> methodSet = new HashSet<>(Arrays.asList(
                TRANS_METHOD_MAP,
                TRANS_METHOD_FILTER,
                TRANS_METHOD_SAMPLE,
                TRANS_METHOD_UNION,
                TRANS_METHOD_INTERSECTION,
                TRANS_METHOD_DISTINCT,
                TRANS_METHOD_GROUP,
                TRANS_METHOD_REDUCE,
                TRANS_METHOD_SORT,
                TRANS_METHOD_JOIN,
                TRANS_METHOD_PARTITION
        ));

        if(!methodSet.contains(function)){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.FUNCTION_NOT_SUPPORTED, null);
        }

        if(!CommonUtil.valiateStringType(parameterList, 0, 1000)){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        if(!CommonUtil.validateUserIdentity(accessToken)){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, "");
        }

        TransformatEntryResponse result = null;

        switch(function){
            case TRANS_METHOD_MAP:
                result = this.transMap(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_FILTER:
                result = this.transFilter(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_SAMPLE:
                result = this.transSample(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_UNION:
                result = this.transUnion(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_INTERSECTION:
                result = this.transIntersection(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_DISTINCT:
                result = this.transDistinct(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_GROUP:
                result = this.transGroupByKey(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_REDUCE:
                result = this.transReduceByKey(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_SORT:
                result = this.transSortByKey(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_JOIN:
                result = this.transJoin(distributedDataset, parameterList, accessToken);
                break;
            case TRANS_METHOD_PARTITION:
                result = this.transPartition(distributedDataset, parameterList, accessToken);
                break;
        }

        return result;
    }

    private ActionEntryResponse collect(Object distributedDataset, String parameterList, String accessToken){
        CollectResponse resp = this.collect(distributedDataset, accessToken);
        return new ActionEntryResponse(
                resp.getTaskStatus(),
                resp.getResult(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public CollectResponse collect(Object distributedDataset, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return CollectResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        try {
            String submitRequestJson = new SubmitRequest((String) distributedDataset).toJson();
            String result = sendRequest("/action/collect", submitRequestJson);
            return CollectResponse.convertJsonToResponse(result);
        } catch (Exception e){
            e.printStackTrace();
            return CollectResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    @Override
    public CountResponse count(Object distributedDataset, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return CountResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, 0);
        }

        try {
            String submitRequestJson = new SubmitRequest((String) distributedDataset).toJson();
            String result = sendRequest("/action/count", submitRequestJson);
            return CountResponse.convertJsonToResponse(result);
        } catch (Exception e){
            e.printStackTrace();
            return CountResponse.getResponse(ErrorCodeEnum.FAILED, 0);
        }
    }

    private ActionEntryResponse count(Object distributedDataset, String parameterList, String accessToken){
        CountResponse resp = this.count(distributedDataset, accessToken);
        return new ActionEntryResponse(
                resp.getTaskStatus(),
                resp.getTotalNum(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public TakeResponse take(Object distributedDataset, int amount, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return TakeResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        try {
            String requestJson = new TakeRequest((String) distributedDataset, amount).toJson();
            String result = sendRequest("/action/take", requestJson);
            return TakeResponse.convertJsonToResponse(result);
        } catch (Exception e){
            e.printStackTrace();
            return TakeResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }

    }

    private ActionEntryResponse take(Object distributedDataset, String parameterList, String accessToken){
        JSONObject jsonObject = JSON.parseObject(parameterList);
        if(null == jsonObject){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        Integer amount = jsonObject.getInteger("amount");
        if(null == amount){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        TakeResponse resp = this.take(distributedDataset, amount, accessToken);
        return new ActionEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public Response saveFile(Object distributedDataset, String fileType, String filePath, String accessToken){
        fileType = fileType.toLowerCase();

        if(!this.checkFilType(fileType)){
            return Response.getResponse(ErrorCodeEnum.FILE_TYPE_NOT_SUPPORTED);
        }

        if(!CommonUtil.validateUserIdentity(accessToken)){
            return Response.getResponse(ErrorCodeEnum.PERMISSION_DENIED);
        }

        if(!CommonUtil.valiateStringType(filePath, 0, 1024)){
            return Response.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH);
        }

        if (filePath.startsWith("dfs")) {
            filePath = "hdfs" + filePath.substring(3);
        }

        try{
            String requestJson = new SaveFileRequest((String)distributedDataset, fileType, filePath).toJson();
            String result = sendRequest("/action/saveFile", requestJson);
            SaveFileResponse saveFileResp =  SaveFileResponse.convertJsonToResponse(result);
            if (filePath.startsWith("file://")) {
                filePath = filePath.substring(7);
                CommonUtil.writeDataToLocalFile(filePath, saveFileResp.getDistributedDataset());
            }

            return new Response(
                    saveFileResp.getTaskStatus(),
                    saveFileResp.getErrorCode(),
                    saveFileResp.getErrorMsg()
            );
        }catch (Exception e){
            e.printStackTrace();
            return Response.getResponse(ErrorCodeEnum.SUCCESS);
        }

    }

    private ActionEntryResponse saveFile(Object distributedDataset, String parameterList, String accessToken){
        JSONObject jsonObject = JSON.parseObject(parameterList);
        if(null == jsonObject){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }
        String fileType = jsonObject.getString("fileType").toLowerCase();
        String filePath = jsonObject.getString("filePath");
        if(null == fileType||null == filePath){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        if(!this.checkFilType(fileType)){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.FILE_TYPE_NOT_SUPPORTED, null);
        }

        if(!CommonUtil.validateUserIdentity(accessToken)){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        if(!CommonUtil.valiateStringType(filePath, 0, 1024)){
            return ActionEntryResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        Response resp = this.saveFile(distributedDataset, fileType, filePath, accessToken);
        return new ActionEntryResponse(
                resp.getTaskStatus(),
                distributedDataset,
                resp.getErrorCode(),
                resp.getErrorMsg()
        );

    }

    private boolean checkFilType(String fileType){

        final String FILE_TYPE_TXT = "txt";
        final String FILE_TYPE_CSV = "csv";
        final String FILE_TYPE_ORC = "orc";
        final String FILE_TYPE_PARQUET = "parquet";
        final String FILE_TYPE_JSON = "json";

        Set<String> fileTypeSet = new HashSet<>(Arrays.asList(
                FILE_TYPE_TXT,
                FILE_TYPE_CSV,
                FILE_TYPE_ORC,
                FILE_TYPE_PARQUET,
                FILE_TYPE_JSON
        ));

        return fileTypeSet.contains(fileType);
    }

    @Override
    public MapResponse map(Object distributedDataset, String userDefinedFunction, String accessToken){

        if(!CommonUtil.validateUserIdentity(accessToken)){
            return MapResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        userDefinedFunction = "com.qingcloud.MapObject.udfMap";

        HashMap<String, String> udfInfoMap = CommonUtil.parseClassAndMethod(userDefinedFunction);
        if(udfInfoMap == null){
            return MapResponse.getResponse(ErrorCodeEnum.USER_DEFINED_FUNCTION_PATH_NOT_VALID, null);
        }

        try{
            String requestJson = new MapRequest((String) distributedDataset, userDefinedFunction).toJson();
            String result = sendRequest("/transformation/map", requestJson);
            return MapResponse.convertJsonToResponse(result);

        }catch (Exception e){
            e.printStackTrace();
            return MapResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transMap(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        String userDefinedFunction = paramJsonObject.getString("userDefinedFunction");

        if(null == distributedDataset || null == userDefinedFunction){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        MapResponse resp = this.map(distributedDataset, userDefinedFunction, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public FilterResponse filter(Object distributedDataset, String userDefinedFunction, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return FilterResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        userDefinedFunction = "com.qingcloud.MapObject.udfFilter";

        HashMap<String, String> udfInfoMap = CommonUtil.parseClassAndMethod(userDefinedFunction);
        if(udfInfoMap == null){
            return FilterResponse.getResponse(ErrorCodeEnum.USER_DEFINED_FUNCTION_PATH_NOT_VALID, null);
        }

        try{
            String requestJson = new FilterRequest((String) distributedDataset, userDefinedFunction).toJson();
            String result = sendRequest("/transformation/filter", requestJson);
            return FilterResponse.convertJsonToResponse(result);
        }catch (Exception e){
            e.printStackTrace();
            return FilterResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transFilter(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        String userDefinedFunction = paramJsonObject.getString("userDefinedFunction");

        if(null == distributedDataset || null == userDefinedFunction){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        FilterResponse resp = this.filter(distributedDataset, userDefinedFunction, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );

    }

    @Override
    public SampleResponse sample(Object distributedDataset, String replace, Float percentage, int randomSeed, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return SampleResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        if(!CommonUtil.valiateStringType(replace, 1, 1)){
            return SampleResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        try{
            String requestJson = new SampleRequest((String) distributedDataset, replace, percentage, randomSeed).toJson();
            String result = sendRequest("/transformation/sample", requestJson);
            return SampleResponse.convertJsonToResponse(result);
        }catch (Exception e){
            e.printStackTrace();
            return SampleResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transSample(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        String replace = paramJsonObject.getString("replace");
        Float percentage = paramJsonObject.getFloat("percentage");
        int randomSeed = paramJsonObject.getInteger("randomSeed");
        if (null == replace || null == percentage || null == distributedDataset){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        SampleResponse resp = this.sample(distributedDataset, replace, percentage, randomSeed, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public UnionResponse union(Object distributedDataset1, Object distributedDataset2, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return UnionResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        try{
            String requestJson = new UnionRequest((String) distributedDataset1, (String) distributedDataset2).toJson();
            String result = sendRequest("/transformation/union", requestJson);
            return UnionResponse.convertJsonToResponse(result);
        }catch (Exception e){
            e.printStackTrace();
            return UnionResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transUnion(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        Object distributedDataset1 = paramJsonObject.getString("distributedDataset1");
        Object distributedDataset2 = paramJsonObject.getString("distributedDataset2");
        if(null == distributedDataset1 || null == distributedDataset2){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        UnionResponse resp = this.union(distributedDataset1, distributedDataset2, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public IntersectionResponse intersection(Object distributedDataset1, Object distributedDataset2, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return IntersectionResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        try{
            String requestJson = new IntersectionRequest((String) distributedDataset1, (String) distributedDataset2).toJson();
            String result = sendRequest("/transformation/intersection", requestJson);
            return IntersectionResponse.convertJsonToResponse(result);
        }catch (Exception e){
            e.printStackTrace();
            return IntersectionResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transIntersection(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        Object distributedDataset1 = paramJsonObject.getString("distributedDataset1");
        Object distributedDataset2 = paramJsonObject.getString("distributedDataset2");

        IntersectionResponse resp = this.intersection(distributedDataset1, distributedDataset2, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }


    @Override
    public DistinctResponse distinct(Object distributedDataset, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return DistinctResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        try {
            String submitRequestJson = new SubmitRequest((String) distributedDataset).toJson();
            String result = sendRequest("/transformation/distinct", submitRequestJson);
            return DistinctResponse.convertJsonToResponse(result);
        } catch (Exception e){
            e.printStackTrace();
            return DistinctResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transDistinct(Object distributedDataset, String parameterList, String accessToken){
        DistinctResponse resp = this.distinct(distributedDataset, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public GroupByKeyResponse groupByKey(Object distributedDataset, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return GroupByKeyResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        try {
            String submitRequestJson = new SubmitRequest((String) distributedDataset).toJson();
            String result = sendRequest("/transformation/groupByKey", submitRequestJson);
            return GroupByKeyResponse.convertJsonToResponse(result);
        } catch (Exception e){
            e.printStackTrace();
            return GroupByKeyResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transGroupByKey(Object distributedDataset, String parameterList, String accessToken){
        GroupByKeyResponse resp = this.groupByKey(distributedDataset, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }


    @Override
    public ReduceByKeyResponse reduceByKey(Object distributedDataset, String reduceFunction, String keyField, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return ReduceByKeyResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        reduceFunction = "udf.Udf.udfReduce";

        if(!CommonUtil.valiateStringType(reduceFunction, 0, 20)){
            return ReduceByKeyResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        if(!CommonUtil.valiateStringType(keyField, 0, 32)){
            return ReduceByKeyResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        HashMap<String, String> udfInfoMap = CommonUtil.parseClassAndMethod(reduceFunction);
        if(udfInfoMap == null){
            return ReduceByKeyResponse.getResponse(ErrorCodeEnum.USER_DEFINED_FUNCTION_PATH_NOT_VALID, null);
        }

        try{
            String reduceRequestJson = new ReduceByKeyRequest((String) distributedDataset, reduceFunction, keyField).toJson();
            String result = sendRequest("/transformation/reduceByKey", reduceRequestJson);
            return ReduceByKeyResponse.convertJsonToResponse(result);
        }catch (Exception e){
            return ReduceByKeyResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }


    private TransformatEntryResponse transReduceByKey(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        String reduceFunction = paramJsonObject.getString("reduceFunction");
        String keyField = paramJsonObject.getString("keyField");
        if(null == reduceFunction){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        ReduceByKeyResponse resp = null;
        resp = this.reduceByKey(distributedDataset, reduceFunction, keyField, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public SortByKeyResponse sortByKey(Object distributedDataset, String sort, String keyField, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return SortByKeyResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        if(!CommonUtil.valiateStringType(sort, 1, 1)){
            return SortByKeyResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        if(!CommonUtil.valiateStringType(keyField, 0, 32)){
            return SortByKeyResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        try{
            String sortRequestJson = new SortByKeyRequest((String) distributedDataset, sort, keyField).toJson();
            String result = sendRequest("/transformation/sortByKey", sortRequestJson);
            return SortByKeyResponse.convertJsonToResponse(result);
        }catch (Exception e){
            return SortByKeyResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transSortByKey(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        String sort = paramJsonObject.getString("sort");
        String keyField = paramJsonObject.getString("keyField");
        if(null == sort){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        SortByKeyResponse resp = null;
        resp = this.sortByKey(distributedDataset, sort, keyField, accessToken);

        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public JoinResponse join(Object distributedDataset1, Object distributedDataset2, int joinMethod, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return JoinResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        final int METHOD_LEFT_OUTER_JOIN = 0;
        final int METHOD_RIGHT_OUTER_JOIN = 1;
        final int METHOD_INNER_JOIN = 2;
        final int METHOD_OUTER_JOIN = 3;
        Set<Integer> methodNumSet = new HashSet<>(Arrays.asList(
                METHOD_LEFT_OUTER_JOIN, METHOD_RIGHT_OUTER_JOIN, METHOD_INNER_JOIN, METHOD_OUTER_JOIN)
        );

        if(!methodNumSet.contains(joinMethod)){
            return JoinResponse.getResponse(ErrorCodeEnum.PARAM_OUT_OF_RANGE, null);
        }

        try{
            String requestJson = new JoinRequest((String) distributedDataset1, (String) distributedDataset2, joinMethod).toJson();
            String result = sendRequest("/transformation/join", requestJson);
            return JoinResponse.convertJsonToResponse(result);
        }catch (Exception e){
            return JoinResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transJoin(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == paramJsonObject){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        Object distributedDataset1 = paramJsonObject.getString("distributedDataset1");
        Object distributedDataset2 = paramJsonObject.getString("distributedDataset2");

        Integer joinMethod = paramJsonObject.getInteger("joinMethod");
        if(null == joinMethod){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        JoinResponse resp = this.join(distributedDataset1, distributedDataset2, joinMethod, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }

    @Override
    public PartitionResponse partition(Object distributedDataset, int partitionNumber, String accessToken){
        if(!CommonUtil.validateUserIdentity(accessToken)){
            return PartitionResponse.getResponse(ErrorCodeEnum.PERMISSION_DENIED, null);
        }

        if (!CommonUtil.validateIntType(partitionNumber, 32)){
            return PartitionResponse.getResponse(ErrorCodeEnum.INVALID_INPUT_PARAMS_LENGTH, null);
        }

        try{
            String requestJson = new PartitionRequest((String) distributedDataset, partitionNumber).toJson();
            String result = sendRequest("/transformation/partition", requestJson);
            return PartitionResponse.convertJsonToResponse(result);
        }catch (Exception e){
            return PartitionResponse.getResponse(ErrorCodeEnum.FAILED, null);
        }
    }

    private TransformatEntryResponse transPartition(Object distributedDataset, String parameterList, String accessToken){
        JSONObject paramJsonObject = JSON.parseObject(parameterList);
        if(null == parameterList){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        Integer partitionNumber = paramJsonObject.getInteger("partitionNumber");
        if(null == partitionNumber){
            return TransformatEntryResponse.getResponse(ErrorCodeEnum.PARAM_NOT_FOUND, null);
        }

        PartitionResponse resp = this.partition(distributedDataset, partitionNumber, accessToken);
        return new TransformatEntryResponse(
                resp.getTaskStatus(),
                resp.getDistributedDataset(),
                resp.getErrorCode(),
                resp.getErrorMsg()
        );
    }


    public static void main(String[] args) throws Exception{
        String funcPath = "paas.computation.memoryComputation.MemoryComputationImpl.myPrint";
        HashMap<String, String> udfInfoMap = CommonUtil.parseClassAndMethod(funcPath);
        String udfClassPath = udfInfoMap.get("ClassPath");
        String methodName = udfInfoMap.get("FuncName");

        Class clazz = Class.forName(udfClassPath);
        Method[] methods = clazz.getMethods();

        Method udfMethod = null;
        for(int i=0;i<methods.length;i++){
            if(methodName.equals(methods[i].getName())){
                udfMethod = methods[i];
                break;
            }
        }

        udfMethod.invoke(clazz.newInstance(), "hello world");
        int tmpNum = 234434;
        System.out.println(String.valueOf(tmpNum).length());

    }

    public String myPrint(String input){
        System.out.println(String.format("in MyPrint ==> %s", input));
        return "ok";
    }

}
