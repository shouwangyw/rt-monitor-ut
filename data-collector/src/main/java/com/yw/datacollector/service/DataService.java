package com.yw.datacollector.service;

import com.yw.datacollector.exception.CustomException;
import com.yw.datacollector.response.RestResponseCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class DataService {

    public void process(String dataType, HttpServletRequest request) {
        // 校验数据类型
        if (StringUtils.isBlank(dataType)) {
            throw new CustomException(RestResponseCode.LOG_TYPE_ERROR);
        }
        // 校验请求头中是否传入数据
        int contentLength = request.getContentLength();
        if (contentLength < 1) {
            throw new CustomException(RestResponseCode.REQUEST_CONTENT_INVALID);
        }

        // 从请求request中读取数据
        byte[] bytes = new byte[contentLength];

        try (BufferedInputStream bis = new BufferedInputStream(request.getInputStream())){
            // 最大尝试读取的次数
            int maxTryTimes = 100;
            int tryTimes = 0, totalReadLength = 0;
            while (totalReadLength < contentLength && tryTimes < maxTryTimes) {
                int readLength = bis.read(bytes, totalReadLength, contentLength - totalReadLength);
                if (readLength < 0) {
                    throw new CustomException(RestResponseCode.BAD_NETWORK);
                }
                totalReadLength += readLength;
                if (totalReadLength == contentLength) {
                    break;
                }
                tryTimes++;
                TimeUnit.MILLISECONDS.sleep(200);
            }
            if (totalReadLength < contentLength) {
                // 经过多处的读取，数据仍然没有读完
                throw new CustomException(RestResponseCode.BAD_NETWORK);
            }
            String jsonData = new String(bytes, StandardCharsets.UTF_8);
            log.info(jsonData);
        } catch (Exception e) {
            throw new CustomException(RestResponseCode.SYSTEM_ERROR, e.getMessage());
        }
    }
}
