package com.madhouse.dsp;

/**
 * Created by Madhouse on 2017/9/21.
 */
public class MyThread extends Thread {
    private String name;
    public MyThread(String n){
        name = n;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "正在执行的任务是：" + name);
    }
}
