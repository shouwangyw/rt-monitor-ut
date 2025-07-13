package com.yw.datacollector.mock;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class SendDataToServer {
    public static void main(String[] args) throws Exception {
        String url = "http://localhost:8686/api/sendData/traffic_data";
        HttpClient client = HttpClients.createDefault();
        int i = 0;
        while (i < 20) {
            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/json");
            String data = "11,22,33,京P12345,57.2," + i;
            post.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
            HttpResponse response = client.execute(post); //发送数据
            i++;
            TimeUnit.SECONDS.sleep(1);

            //响应的状态如果是200的话，获取服务器返回的内容
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String result = EntityUtils.toString(response.getEntity(), "UTF-8");
                System.out.println(result);
            }
        }
    }
}
