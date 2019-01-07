import java.util.Stack;

public class MinStack {

    Stack<Integer> s = new Stack<Integer>();
    int min = Integer.MIN_VALUE;
    public MinStack() {
        // do intialization if necessary
    }

    /*
     * @param number: An integer
     * @return: nothing
     */
    public void push(int number) {
        // write your code here
        if(number<=min){
            s.push(min);
            min = number;
        }
        s.push(number);
    }

    /*
     * @return: An integer
     */
    public int pop() {
        // write your code here
        int p = s.pop();
        if(p==min){
            min = s.pop();
        }
        return p;
    }

    /*
     * @return: An integer
     */
    public int min() {
        // write your code here
        return min;
    }

    /*Stack<Integer> s = new Stack<Integer>();
    Stack<Integer> min = new Stack<Integer>();
    public MinStack() {
        // do intialization if necessary

    }

    *//*
     * @param number: An integer
     * @return: nothing
     *//*
    public void push(int number) {
        // write your code here
        s.push(number);
        if(min.empty() || number<=min.peek()){
            min.push(number);
        }
    }

    *//*
     * @return: An integer
     *//*
    public int pop() {
        // write your code here
        int p = s.pop();
        if(p==min.peek()){
            min.pop();
        }
        return p;
    }

    *//*
     * @return: An integer
     *//*
    public int min() {
        // write your code here
        return min.peek();
    }*/
}
