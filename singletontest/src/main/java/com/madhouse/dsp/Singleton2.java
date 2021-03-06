package com.madhouse.dsp;

/**
 * Created by Madhouse on 2017/9/21.
 */
public class Singleton2 {
    /**
     * 对保存实例的变量添加volatile的修饰
     */
    private volatile static Singleton2 instance = null;

    private Singleton2() {
    }

    public static Singleton2 getInstance() {
        //先检查实例是否存在，如果不存在才进入下面的同步块
        if (instance == null) {
            //同步块，线程安全的创建实例
            synchronized (Singleton2.class) {
            //再次检查实例是否存在，如果不存在才真的创建实例
                if (instance == null) {
                    instance = new Singleton2();
                }
            }
        }
        return instance;
    }
}
