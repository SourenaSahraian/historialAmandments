package com.viaplay.historicalamendment;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("dynammo")
public class ApplicationConfig {

    @Getter @Setter
    private String streamProgress;
    @Getter @Setter
    private String ContinueWatching;
    @Getter @Setter
    private String userProgramMarks;

    @Getter @Setter
    private String UserProgramMarksWatched;

}
