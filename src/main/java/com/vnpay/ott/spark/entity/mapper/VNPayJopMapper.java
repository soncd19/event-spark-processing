package com.vnpay.ott.spark.entity.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vnpay.ott.spark.entity.VNPayJob;

/**
 * Created by SonCD on 13/05/2021
 */
public class VNPayJopMapper {

    public static VNPayJob mapper(String data) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(data, VNPayJob.class);
    }
}
