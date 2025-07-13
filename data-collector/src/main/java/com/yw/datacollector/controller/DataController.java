package com.yw.datacollector.controller;

import com.yw.datacollector.response.RestResponse;
import com.yw.datacollector.service.DataService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 * 数据采集服务器接口
 * API：http://localhost:8686/api/sendData/carInfo
 * 数据的具体内容通过请求传入
 */
@RestController
@RequestMapping("/api")
public class DataController {

    @Resource
    private DataService dataService;

    @PostMapping("/sendData/{dataType}")
    public RestResponse<Void> collect(@PathVariable("dataType") String dataType,
                                      HttpServletRequest request) {

        dataService.process(dataType, request);
        return RestResponse.success();
    }
}
