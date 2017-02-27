package cc.xidian.MainTest;

import java.util.Scanner;

/**
 * Created by hadoop on 2017/2/16.
 */
public class Main {
//    public static boolean Find(int target, int [][] array) {
//        int length = array.length;
//
//        boolean found = false;
//        if(rowNum>0 && columnNum > 0){
//            int row = rowNum - 1;
//            int column = 0;
//            while(row>0&&column<columnNum){
//                if(array[column][row] == target){
//                    found = true;
//                    break;
//                }else if(array[column][row] > target){
//                    column --;
//                }else{
//                    row ++;
//                }
//            }
//        }
//        return found;
//    }
    public static void main(String[] args){

        int[][] array = new int[5][3];
        for(int i=0;i<5;i++){
            for(int j=0;j<3;j++){
                array[i][j] = 3;
            }
        }
        System.out.println(array.length);
        System.out.println(array[0].length);
//        Scanner in = new Scanner(System.in);
//        while(in.hasNextLine()){
//
//            String str = in.nextLine();
//            System.out.println(str);
////            int a = in.nextInt();
////            int b = in.nextInt();
////            System.out.println(a+b);
//
//        }
    }
}
