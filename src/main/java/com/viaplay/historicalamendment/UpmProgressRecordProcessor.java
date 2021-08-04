package com.viaplay.historicalamendment;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.viaplay.historicalamendment.model.RecordDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@Component
public class UpmProgressRecordProcessor implements RecordProcessor {

    private Table upmInProgressTable = null;

    @Autowired
    private DynamoDB dynamoDB;

    @Autowired
    private ApplicationConfig config;


    @PostConstruct
    public void preLoadDataBases() {
        upmInProgressTable = dynamoDB.getTable(config.getUserProgramMarks());

    }

    @Override
    public <T> CompletableFuture<T>  createUpdateQuery(RecordDao recordDao) {


        return (CompletableFuture<T>) CompletableFuture.supplyAsync(() -> {

            System.out.println( "from UPM progress:  " );
            String userId = createHashKey(recordDao);
                HashMap<String, AttributeValue> upmProgressPrimaryKey = new HashMap<>();
                upmProgressPrimaryKey.put("userId", new AttributeValue(userId));
                upmProgressPrimaryKey.put("programGuid", new AttributeValue(recordDao.getProgramGuid()));

                return new Delete()
                        .withTableName(config.getUserProgramMarks()).
                                withKey(upmProgressPrimaryKey);



        });


    }
}

