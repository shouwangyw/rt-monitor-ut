package com.yw.datacollector.response;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author yangwei
 */
@Data
@NoArgsConstructor
public class RestResponse<T> implements Serializable {
    private T data;
    private String code;
    private String msg;

    public RestResponse(RestResponseCode code) {
        this.code = code.getCode();
        this.msg = code.getMsg();
    }

    public RestResponse(RestResponseCode code, String msg) {
        this.code = code.getCode();
        this.msg = msg;
    }

    public RestResponse(RestResponseCode code, T data) {
        this.code = code.getCode();
        this.msg = code.getMsg();
        this.data = data;
    }

    public RestResponse(String code, T data, String msg) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public static <T> RestResponse<T> success() {
        return new RestResponse<>(RestResponseCode.SUCCESS);
    }

    public static <T> RestResponse<T> success(T data) {
        return new RestResponse<>(RestResponseCode.SUCCESS, data);
    }

    public static <T> RestResponse<T> success(T data, String msg) {
        return new RestResponse<>(RestResponseCode.SUCCESS.getCode(), data, msg);
    }

    public static <T> RestResponse<T> error(String msg) {

        return new RestResponse<>(RestResponseCode.SYSTEM_ERROR, msg);
    }

    public static <T> RestResponse<T> error(RestResponseCode code) {

        return new RestResponse<>(code);
    }

    public static <T> RestResponse<T> error(RestResponseCode code, String msg) {
        return new RestResponse<>(code, msg);
    }

}
