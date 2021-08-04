package com.viaplay.historicalamendment;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@Qualifier("spRecordProcessor")
public class SpRecordProcessor implements RecordProcessor {

    @Autowired
    private ApplicationConfig config;

    private Table spTable = null;

    @Autowired
    private DynamoDB dynamoDB;

    @PostConstruct
    public void preLoadDataBases() {
        spTable = dynamoDB.getTable(config.getStreamProgress());

    }

    @Override
    public String createHashKey(String userId, String profileId, String programGuid,  boolean isKids) {
        if (userId.equalsIgnoreCase(profileId)) {
            userId = profileId;
        } else {
            String semicolon = "::";
            userId = "viaplay" + semicolon + userId + semicolon + profileId;
        }
        return userId;
    }

    public boolean shouldUpdateRecord(String rawUserId, String profileId, String rawProgramGuid, boolean isKids) {
        String userId = createHashKey(rawUserId, profileId, rawProgramGuid, isKids);
        boolean shouldupdate = false;
        try {
            GetItemSpec itemSpec = new GetItemSpec()
                    .withPrimaryKey(new PrimaryKey("userId", userId, "programGuid", rawProgramGuid));
            Item spRecord = spTable.getItem(itemSpec);
            System.out.println(spRecord.toJSONPretty());

            Long position = spRecord.getLong("position");
            Long duration = spRecord.getLong("duration");
            String completed = spRecord.getString("completed");
             shouldupdate = (position >= (WATCHED_THRESHOLD * duration));
            System.out.println("should continue ?  " + shouldupdate);
        } catch(Exception ex) {
            System.out.println( " error fecthing the SP record " + ex.getMessage());
        }



        return shouldupdate;
    }




    @Override
    public <T> CompletableFuture<T> createUpdateQuery(String rawUserId, String profileId, String rawProgramGuid, String seriesGuid , boolean isKids) {


        return (CompletableFuture<T>) CompletableFuture.supplyAsync(() -> {

            String userId = createHashKey(rawUserId, profileId, rawProgramGuid, isKids);
                HashMap<String, AttributeValue> spPrimaryKey = new HashMap<>();
                spPrimaryKey.put("userId", new AttributeValue(userId)); //TODO chage to userId
                spPrimaryKey.put("programGuid", new AttributeValue(rawProgramGuid)); //TODO chage to userId

                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(":new_status", new AttributeValue("true"));

                System.out.println("from SP ");
                return new Update()
                        .withTableName(config.getStreamProgress())
                        .withKey(spPrimaryKey)
                        .withUpdateExpression("SET completed = :new_status")
                        .withExpressionAttributeValues(expressionAttributeValues)
                        .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD);





        });


    }
}