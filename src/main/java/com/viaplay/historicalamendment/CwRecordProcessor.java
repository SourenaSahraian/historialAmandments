package com.viaplay.historicalamendment;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class CwRecordProcessor implements RecordProcessor {


    private Table cwTable = null;

    @Autowired
    private DynamoDB dynamoDB;

    @Autowired
    private ApplicationConfig config;


    @PostConstruct
    public void preLoadDataBases() {
        cwTable = dynamoDB.getTable(config.getContinueWatching());

    }

    @Override
    public String createHashKey(String userId, String profileId, String programGuid , boolean isKids) {
        //RecordProcessor.super.createUserId(userId;);
        String suffix =  isKids ? "children": "default";
        String semicolon = "::";
        if (userId.equalsIgnoreCase(profileId)) {
            userId = userId  + semicolon + suffix;
        } else {
            userId = "viaplay" + semicolon + userId + semicolon + profileId + semicolon + suffix;
        }
        return userId;

    }


    private String createRangeKey(String seriesGuid, String rawProgramGuid){
        return seriesGuid.equalsIgnoreCase("") ? rawProgramGuid :seriesGuid ;
    }

    public boolean shouldUpdateRecord(String rawUserId, String profileId, String rawProgramGuid, String seriesGuid, boolean isKids) {

        String hashKey = createHashKey(rawUserId, profileId, rawProgramGuid, isKids);
        System.out.println( "from should update in CW11 hashkey" + hashKey + " - and program guid is :" + rawProgramGuid);
        String secondaryKey = createRangeKey(seriesGuid,rawProgramGuid ) ;
        Item cwRecord = null;
        try {
            GetItemSpec itemSpec = new GetItemSpec()
                    .withPrimaryKey(new PrimaryKey("userId_sectionId", hashKey, "guid", secondaryKey));

             cwRecord = cwTable.getItem(itemSpec);
       } catch(Exception ex){
            ex.printStackTrace();
       }
        System.out.println( "from CW result" + cwRecord);
        if(cwRecord != null){
            String programGuid = cwRecord.getString("programGuid");
            Boolean completed = Boolean.valueOf(cwRecord.getString("completed"));
            System.out.println( "from CW " + cwRecord.toJSONPretty());
            if(rawProgramGuid.equalsIgnoreCase(programGuid) && completed == false){
                return true;
            }
        }

        return false;
    }

    @Override
    public CompletableFuture<Update> createUpdateQuery(String rawUserId, String profileId, String rawProgramGuid, String seriesGuid, boolean isKids) {
        //TODO check the condition -  return update query o.w null

        return CompletableFuture.supplyAsync(() -> {
            System.out.println("running CW");
            if(!shouldUpdateRecord(rawUserId, profileId, rawProgramGuid, seriesGuid, true)){
                return null;
            }

            String secondaryKey = createRangeKey(seriesGuid,rawProgramGuid ) ;
            String hashKey = createHashKey(rawUserId, profileId, rawProgramGuid,isKids);
            HashMap<String, AttributeValue> spPrimaryKey = new HashMap<>();
            spPrimaryKey.put("userId_sectionId", new AttributeValue(hashKey));
            spPrimaryKey.put("guid", new AttributeValue(secondaryKey));

            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":new_status", new AttributeValue("true"));


            return new Update()
                    .withTableName(config.getContinueWatching())
                    .withKey(spPrimaryKey)
                    .withUpdateExpression("SET completed = :new_status")
                    .withExpressionAttributeValues(expressionAttributeValues)
                    .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD);


        });


    }
}