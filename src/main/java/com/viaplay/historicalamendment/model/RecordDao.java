package com.viaplay.historicalamendment.model;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Timestamp;

@Builder
@Getter
@Setter
public class RecordDao {

    private String userId;
    private String profileId;
    private String  programGuid ;
    private String  seriesGuid ;
    private boolean isKids = false;
    private String timestamp;
}
