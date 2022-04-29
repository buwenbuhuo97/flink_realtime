package com.buwenbuhuo.gmallpublisher.controller;

import com.buwenbuhuo.gmallpublisher.service.GmvService;
import com.buwenbuhuo.gmallpublisher.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Author 不温卜火
 * Create 2022-04-28 15:31
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Controller层代码实现
 */

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    /**
     * 这是一个测试类
     * 测试地址：http://localhost:8070/test1
     *
     * @return {"id":"1001","name":"buwenbuhuo"}
     */
    @RequestMapping("/test1")
    public String test1() {
        System.out.println("是否成功被调用");
        // return "success";
        return "{\"id\":\"1001\",\"name\":\"buwenbuhuo\"}";
    }

    /**
     * 测试2
     * 测试地址：http://localhost:8070/test2?nn=buwenbuhuo&age=18
     *
     * @return {name: "buwenbuhuo",age: "18"}
     */
    @RequestMapping("/test2")
    public String test2(@RequestParam("nn") String name,
                        @RequestParam("age") int age) {
        System.out.println(name + ":" + age);
        return "success";
        // return "{\"name\":\"buwenbuhuo\",\"age\":\"18\"}";
    }

    /**
     * 测试3
     * 测试地址1：http://localhost:8070/test3?nn=buwenbuhuo&age=20
     * 测试地址2：http://localhost:8070/test3?nn=buwenbuhuo&age=18
     * 测试地址3：http://localhost:8070/test3?nn=buwenbuhuo
     *
     * @return 1. {"name":"buwenbuhuo","age":"20"}
     * 2. {"name":"buwenbuhuo","age":"18"}
     * 3. {"name":"buwenbuhuo","age":"18"}
     */
    @RequestMapping("/test3")
    public String test3(@RequestParam("nn") String name,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(name + ":" + age);
        return "success";
        //return "{\"name\":\"buwenbuhuo\",\"age\":\"18\"}";
    }

    @Autowired
    private GmvService gmvService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        // 查询数据
        Double gmv = gmvService.getGmv(date);

        // 拼接并返回结果数据
        return "{ " +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": " + gmv +
                "}";
    }


    @Autowired
    private UvService uvService;

    @RequestMapping("/ch")
    public String getUvByCh(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        // 获取数据
        Map uvByCh = uvService.getUvByCh(date);
        Set chs = uvByCh.keySet();
        Collection uvs = uvByCh.values();

        // 拼接JSON字符串并返回结果
        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\" " +
                StringUtils.join(chs, "\",\"") +
                "   \" ], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"日活\", " +
                "        \"data\": [ " +
                StringUtils.join(uvs, ",") +
                "        ] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }

}
