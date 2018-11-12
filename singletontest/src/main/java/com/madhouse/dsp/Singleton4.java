package com.madhouse.dsp;

/**
 * Created by Madhouse on 2017/9/21.
 */
public class Singleton4 {
    /**
     * 类级的内部类，也就是静态的成员式内部类，该内部类的实例与外部类的实例
     * 没有绑定关系，而且只有被调用到才会装载，从而实现了延迟加载
     */
    private static class SingletonHolder{
        /**
         * 静态初始化器，由JVM来保证线程安全
         */
        private static Singleton4 instance = new Singleton4();
    }

    /**
     * 私有化构造方法
     */
    private Singleton4(){}

    public static  Singleton4 getInstance(){
        return SingletonHolder.instance;
    }
}
