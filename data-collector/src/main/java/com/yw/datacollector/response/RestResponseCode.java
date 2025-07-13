package com.yw.datacollector.response;

/**
 * @author yangwei
 */
public enum RestResponseCode {
    /**
     *
     */
    SUCCESS("S0000", "Success"),
    /**
     * 错误码
     */
    LOG_TYPE_ERROR("E0001", "Log type is error"),
    COMPRESS_FLAG_ERROR("E0002", "Compress flag error"),
    REQUEST_CONTENT_INVALID("E0003", "Request content is invalid"),
    DECOMPRESS_FAIL("E0004", "Decompress failed"),
    JSON_PARSE_FAIL("E0005", "Json parse failed"),
    LOG_FAIL("E0006", "Log failed"),
    SIGN_CHECK_ERROR("E0007", "Sign check error"),


    BAD_NETWORK("E1000", "Bad network"),
    SYSTEM_ERROR("E9999", "system exception"),
    ;
    private final String code;
    private final String msg;
    RestResponseCode(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
