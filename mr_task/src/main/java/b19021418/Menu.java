package b19021418;


import java.lang.reflect.Method;
import java.util.Scanner;

public class Menu {

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("=========基于MapReduce的学生成绩分析=========");
                System.out.println("1、计算每门成绩的最高分、最低分、平均分");
                System.out.println("2、计算每个学生的总分及平均成绩并进行排序");
                System.out.println("3、统计所有学生的信息");
                System.out.println("4、统计每门课程中相同分数分布情况");
                System.out.println("5、统计各性别的人数及他们的姓名");
                System.out.println("6、统计每门课程信息");
                System.out.println("7、退出");
                System.out.print("请输入你想要选择的功能：");
                int option = scanner.nextInt();
                Method method = null;
                switch (option) {
                    case 1:
                        method = T1.class.getMethod("main", String[].class);
                        method.invoke(null, (Object) new String[]{});
                        break;
                    case 2:
                        method = T2.class.getMethod("main", String[].class);
                        method.invoke(null, (Object) new String[]{});
                        break;
                    case 3:
                        method = T3.class.getMethod("main", String[].class);
                        method.invoke(null, (Object) new String[]{});
                        break;
                    case 4:
                        method = T4.class.getMethod("main", String[].class);
                        method.invoke(null, (Object) new String[]{});
                        break;
                    case 5:
                        method = T5.class.getMethod("main", String[].class);
                        method.invoke(null, (Object) new String[]{});
                        break;
                    case 6:
                        method = T6.class.getMethod("main", String[].class);
                        method.invoke(null, (Object) new String[]{});
                        break;
                    case 7:
                        System.exit(1);
                        break;
                    default:
                        System.out.println("输入正确的功能按键！！");
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
