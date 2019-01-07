import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BitAdd {
    public static int aplusb(int a, int b) {
        // write your code here
        if (b == 0) return a;
        int c = a ^ b;
        int d = (a & b) << 1;
        return aplusb(c, d);
    }

    public static int[] change(int a, int b) {
        // write your code here
        int r[] = {0, 0};
        if (a != b) {
            int c = a ^ b;
            a = c ^ a;
            b = c ^ a;
        }
        r[0] = a;
        r[1] = b;
        return r;
    }

    public static long trailingZeros(long n) {
        // write your code here, try to do it without arithmetic operators.
        if (n < 5)
            return 0;
        else {
            long k = n / 5;
            return k + trailingZeros(k);
        }
    }

    public static int digitCounts(int k, int n) {
        // write your code here
        int r = 0;
        for (int i = 0; i <= n; i++) {
            int t = i;
            while (t >= 10) {
                if (t % 10 == k) {
                    r += 1;
                }
                t /= 10;
            }
            if (t == k)
                r += 1;
        }

        return r;
    }

    public static int nthUglyNumber(int n) {
        // write your code here
        if (n == 0)
            return 0;
        int[] l = new int[n];
        l[0] = 1;
        int c2 = 0, c3 = 0, c5 = 0;
        int u2, u3, u5;
        for (int i = 1; i < n; i++) {
            u2 = l[c2] * 2;
            u3 = l[c3] * 3;
            u5 = l[c5] * 5;
            l[i] = min(u2, u3, u5);
            if (u2 <= l[i]) {
                c2 += 1;
            }
            if (u3 <= l[i]) {
                c3 += 1;
            }
            if (u5 <= l[i]) {
                c5 += 1;
            }
            System.out.println("l[" + i + "]=" + l[i]);
        }
        return l[n - 1];
    }

    public static int min(int a, int b, int c) {
        int min = a < b ? a : b;
        return min < c ? min : c;
    }

    public static int kthLargestElement(int n, int[] nums) {
        // write your code here
        int l = nums.length;
        if (n == 0 || n > l)
            return 0;
        return qs(n, nums, 0, l - 1);
    }

    public static int qs(int k, int[] nums, int s, int e) {
        if (e == s)
            return nums[e];
        int p = nums[s];
        int l = s, r = e;
        while (r > l) {
            while (r > l && nums[r] <= p) r--;
            if (p < nums[r]) {
                nums[r] = nums[r] ^ nums[l];
                nums[l] = nums[r] ^ nums[l];
                nums[r] = nums[r] ^ nums[l];
            }
            while (l < r && nums[l] >= p) l++;
            if (p > nums[l]) {
                nums[r] = nums[r] ^ nums[l];
                nums[l] = nums[r] ^ nums[l];
                nums[r] = nums[r] ^ nums[l];
            }
        }
        if (l + 1 - s == k)
            return nums[l];
        else if (l + 1 - s > k)
            return qs(k, nums, s, (l - 1));
        else
            return qs(k + s - l - 1, nums, l + 1, e);

    }

    public List<String> fizzBuzz(int n) {
        // write your code here
        List<String> res = new LinkedList<String>();
        for (int i = 1; i <= n; i++) {
            String s = i % 15 == 0 ? "fizzz buzz" : (i % 5 == 0 ? "buzz" : (i % 3 == 0 ? "fizz" : String.valueOf(i)));
            res.add(s);
        }
        return res;
    }

    public static int strStr(String source, String target) {
        // Write your code here
        if (source == null || target == null)
            return -1;
        if (source.equalsIgnoreCase("") && !target.equalsIgnoreCase(""))
            return -1;
        if (source.equals(target) || target.equalsIgnoreCase(""))
            return 0;

        int sl = source.length();
        int tl = target.length();
        int i=0,j=0;
        while(i<sl && j<tl) {
            if (source.charAt(i) == target.charAt(j)) {
                i++;
                j++;
            } else {
                i = i - j + 1;
                j = 0;
            }
            if (j == tl)
                return i - tl;
        }
        return -1;
    }

    public static void main(String[] args) {
        /*int a = 100, b = 12345;
        System.out.println("sum = " + aplusb(a, b));
        int[] r = change(a, b);
        System.out.println("a = " + r[0] + ", b =" + r[1]);*/
        /*int n = 105;
        System.out.println("trailingZeros = " + trailingZeros(n));*/

        /*int k = 1, n = 12;
        int r = digitCounts(k, n);
        System.out.println("digitCounts = " + r);*/

        /*int t = 20;
        System.out.println("nthUglyNumber(" + t + ") = " + nthUglyNumber(t));*/

        /*int n = 105;
        int[] a = {595240, 373125, 463748, 417209, 209393, 747977, 864346, 419023, 925673, 307640, 597868, 833339, 130763, 814627, 766415, 79576, 459038, 990103, 944521, 708820, 473246, 499960, 742286, 758503, 270229, 991199, 770718, 529265, 498975, 721068, 727348, 29619, 712557, 724373, 823743, 318203, 290432, 476213, 412181, 869308, 496482, 793858, 676162, 165869, 160511, 260864, 502521, 611678, 786798, 356560, 916620, 922168, 89350, 857183, 964051, 979979, 916565, 186532, 905289, 653307, 351329, 195491, 866281, 183964, 650765, 675046, 661642, 578936, 78684, 50105, 688326, 648786, 645823, 652329, 961553, 381367, 506439, 77735, 707959, 373271, 316194, 185079, 686945, 342608, 980794, 78777, 687520, 27772, 711098, 661265, 167824, 688245, 286419, 400823, 198119, 35400, 916784, 81169, 874377, 377128, 922531, 866135, 319912, 867697, 10904};
        //int[] a = {9,3,2,4,8};
        //int n = 5;
        System.out.println("res = " + kthLargestElement(n, a));*/

        String s = "abcdabcdefg";
        String t = "bcd";
        System.out.println("res = " + strStr(s, t));

    }
}
