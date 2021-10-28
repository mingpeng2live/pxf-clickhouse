package org.greenplum.pxf.plugins.clickhouse;

public class studentSychronized {

    public  void method_1(){
        System.out.println("方法1启动。。。");
        try {
            synchronized (this){
                System.out.println("方法1执行开。。。");
                Thread.sleep(3000);
                System.out.println("方法1执行中。。。");
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("方法1结束。。。");
    }

    public  void method_2(){
        System.out.println("方法2启动。。。");
        try {
            synchronized (this){
                System.out.println("方法2执行开。。。");
                Thread.sleep(3000);
                System.out.println("方法2执行中。。。");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("方法2结束。。。");
    }

    public static void main(String[] args) {
         studentSychronized student = new studentSychronized();
         studentSychronized student1 = new studentSychronized();

        new Thread(new Runnable() {
            @Override
            public void run() {
                student.method_1();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                student1.method_2();
            }
        }).start();

    }
}