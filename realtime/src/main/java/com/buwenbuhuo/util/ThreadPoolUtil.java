package com.buwenbuhuo.util;

import lombok.SneakyThrows;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author 不温卜火
 * Create 2022-04-26 12:26
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:线程池工具类(单例模式)
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    System.out.println("---创建线程池---");
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }

    // 测试打印
    public static void main(String[] args) {
        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();
        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println("********" + Thread.currentThread().getName() + "********");
                    Thread.sleep(2000);
                }
            });
        }
    }
}

