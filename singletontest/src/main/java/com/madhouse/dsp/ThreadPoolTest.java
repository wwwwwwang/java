package com.madhouse.dsp;

import java.util.concurrent.ExecutorService;

/**
 * Created by Madhouse on 2017/9/21.
 */

public class ThreadPoolTest {
    public static void main(String[] args){
        ExecutorService pool = ThreadPoolFactory.getInstance().getPool("test");
        ExecutorService pool1 = ThreadPoolFactory.getInstance().getPool("test1",3);
        pool.submit(new MyThread("1"));
        pool.submit(new MyThread("2"));
        pool.submit(new MyThread("3"));
        pool.submit(new MyThread("4"));
        pool1.submit(new MyThread("01"));
        pool1.submit(new MyThread("02"));
        pool1.submit(new MyThread("03"));
        pool1.submit(new MyThread("04"));


        pool.submit(() -> {
            final Thread currentThread = Thread.currentThread();
            final String oldName = currentThread.getName();
            currentThread.setName("whsh");
            try {
                System.out.println(Thread.currentThread().getName() + "正在执行的任务是: oldName = " + oldName + ", newName = Processing-whsh");
            } finally {
                currentThread.setName(oldName);
            }
        });

        pool.shutdown();
        pool1.shutdown();
    }
}
