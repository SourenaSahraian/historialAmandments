package com.viaplay.historicalamendment;

import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.Update;

import java.util.concurrent.CompletableFuture;

public interface RecordProcessor {

    double WATCHED_THRESHOLD = 0.95;


    default String createHashKey(String userId, String profileId, String programGuid,  boolean isKids){
        if (userId.equalsIgnoreCase(profileId)) {
            userId = profileId;
        } else {
            String semicolon = "::";
            userId = "viaplay" + semicolon + userId + semicolon + profileId;
        }

        return userId;
    }
     <T> CompletableFuture<T> createUpdateQuery(String userId, String profileId, String programGuid, String seriesGuid, boolean isKids);
}
