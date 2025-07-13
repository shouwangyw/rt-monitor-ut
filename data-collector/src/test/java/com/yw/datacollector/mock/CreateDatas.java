package com.yw.datacollector.mock;

import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class CreateDatas {
    public static void main(String[] args) throws Exception {
//        String url = "http://localhost:8686/api/sendData/traffic_data";
        String url = "http://node3:8686/api/sendData/traffic_data";
        HttpClient client = HttpClients.createDefault();
        String result = null;
        String[] cars = {"京E49174", "京N94279", "京Q76126", "京S96671", "京T14331", "京U54047", "京W85768", "湘M64082", "皖N02828", "皖Q56293", "皖S94997", "皖T74878", "皖Z19534", "鄂Q35888", "鲁O98192", "鲁R41791"};
        String[] locations = {"湘", "京", "京", "京", "京", "京", "京", "鲁", "皖", "鄂"};
        String[] monitorIds = {"0000", "0001", "0002", "0004", "0005", "0021", "0023"};
        // 模拟生成一天的数据
        String day = "2025-06-21";

        // 初始化高斯分布的对象
        JDKRandomGenerator generator = new JDKRandomGenerator();
        generator.setSeed(10);
        GaussianRandomGenerator rg = new GaussianRandomGenerator(generator);

        Random r = new Random();
        // 模拟30000台车
        for (int i = 0; i < 30000; i++) {
            // 得到随机的车牌
            String car = locations[r.nextInt(locations.length)] + (char) (65 + r.nextInt(26)) + String.format("%05d", r.nextInt(100000));
//            String car = cars[r.nextInt(cars.length)];
            // 随机的小时
            String hour = String.format("%02d", r.nextInt(24));
            // 一天内，在一个城市里面，一辆车大概率经过30左右的卡口
            double v = rg.nextNormalizedDouble(); // 标准高斯分布的随机数据，大概率在-1 到 1

            int count = (int) (Math.abs(30 + 30 * v) + 1); // 一辆车一天内经过的卡口数量
            for (int j = 0; j < count; j++) {
                // 如果count超过30，为了数据更加真实，时间要1小时
                if (j % 30 == 0) {
                    int newHour = Integer.parseInt(hour) + 1;
                    if (newHour == 24) {
                        newHour = 0;
                    }
                    hour = String.format("%02d", newHour);
                }
                // 经过卡口的时间
//                String time = day + " " + hour + ":" + String.format("%02d", r.nextInt(60)) + ":" + String.format("%02d", r.nextInt(60));
//                long actionTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
                long actionTime = System.currentTimeMillis();

                // 卡口ID
//                String monitorId = String.format("%04d", r.nextInt(10000));
                String monitorId = monitorIds[r.nextInt(monitorIds.length)];
                // 随机道路
                String roadId = String.format("%02d", r.nextInt(100));
//                String areaId = String.format("%02d", r.nextInt(100));
                String areaId = String.format("%02d", r.nextInt(5));
                String cameraId = String.format("%05d", r.nextInt(100000));

                // 随机速度
                double v2 = rg.nextNormalizedDouble();
                String speed = String.format("%.1f", Math.abs(40 + (40 * v2))); //均值40，标准差是30

                String data = actionTime + "," + monitorId + "," + cameraId + "," + car + "," + speed + "," + roadId + "," + areaId;
                System.out.println(data);
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json");

                post.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
                // 发送数据
                HttpResponse response = client.execute(post);

                TimeUnit.MILLISECONDS.sleep(200);
                // 响应的状态如果是200的话，获取服务器返回的内容
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    result = EntityUtils.toString(response.getEntity(), "UTF-8");
                }
                System.out.println(result);
            }
        }
    }
}
