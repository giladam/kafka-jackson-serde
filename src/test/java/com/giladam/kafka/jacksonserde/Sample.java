package com.giladam.kafka.jacksonserde;

import java.time.Instant;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Something to test deserialization/serialization with.
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Sample {

    @JsonProperty("stringField")
    private String stringField;

    @JsonProperty("dateField")
    private Date dateField;

    @JsonProperty("instantField")
    private Instant instantField;


    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public Date getDateField() {
        return dateField;
    }

    public void setDateField(Date dateField) {
        this.dateField = dateField;
    }

    public Instant getInstantField() {
        return instantField;
    }

    public void setInstantField(Instant instantField) {
        this.instantField = instantField;
    }
}
