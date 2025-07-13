package com.yw.datacollector.exception;

import com.yw.datacollector.response.RestResponseCode;
import lombok.Data;

/**
 * @author yangwei
 */
@Data
public class CustomException extends RuntimeException {
    private RestResponseCode code;
    private String msg;

    public CustomException(String msg) {
        this.code = RestResponseCode.SYSTEM_ERROR;
        this.msg = msg;
    }

    public CustomException(RestResponseCode code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public CustomException(RestResponseCode code) {
        this.code = code;
        this.msg = code.getMsg();
    }
}
