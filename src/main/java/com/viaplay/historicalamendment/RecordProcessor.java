package com.viaplay.historicalamendment;

import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.Update;
import com.viaplay.historicalamendment.model.RecordDao;

import java.util.concurrent.CompletableFuture;

public interface RecordProcessor {

    double WATCHED_THRESHOLD = 0.95;


    default String createHashKey(RecordDao recordDao){
        String userId= recordDao.getUserId();
        String profileId =recordDao.getProfileId();

        if (userId.equalsIgnoreCase(profileId)) {
            userId = profileId;
        } else {
            String semicolon = "::";
            userId = "viaplay" + semicolon + userId + semicolon + profileId;
        }

        return userId;
    }
     <T> CompletableFuture<T> createUpdateQuery(RecordDao recordDao);
}
