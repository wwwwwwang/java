import java.util.ArrayList;
import java.util.List;

public class Permutations15 {
    static List<List<Integer>> res = new ArrayList<List<Integer>>();

    public static List<List<Integer>> permute(int[] nums) {
        // write your code here
        List<Integer> one = new ArrayList<Integer>();
        deal(nums, one);
        return res;
    }

    public static void deal(int[] nums, List<Integer> one) {
        if (nums.length == 1) {
            one.add(nums[0]);
            res.add(new ArrayList<Integer>(one));
            one.remove(one.size() - 1);
        } else {
            for (int num = 0; num < nums.length; num++) {
                one.add(nums[num]);
                deal(filter(nums, num), one);
                one.remove(one.size() - 1);
            }
        }
    }

    public static List<Integer> copy(List<Integer> s) {
        return new ArrayList<Integer>(s);
    }

    public static int[] filter(int[] nums, int n) {
        int length = nums.length;
        int[] ret = new int[length - 1];
        for (int i = 0, j = 0; i < nums.length; i++) {
            if (i != n) {
                ret[j] = nums[i];
                j++;
            }
        }
        return ret;
    }

    public static void pnt(List<List<Integer>> r) {
        for (int i = 0; i < r.size(); i++) {
            for (int j = 0; j < r.get(i).size(); j++) {
                System.out.print(r.get(i).get(j) + " ");
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        int[] a = {1, 2, 3};
        List<List<Integer>> r = permute(a);
        pnt(r);
    }
}
