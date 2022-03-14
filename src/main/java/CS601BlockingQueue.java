/*
    Blocking queue implementation used for the Asynchronous Ordered Dispatch Broker
 */

public class CS601BlockingQueue<T> {
    private T[] items;
    private int start;
    private int end;
    private int size;


    /**
     * Queue is bounded in size.
     * @param size the size of the blocking queue
     */
    public CS601BlockingQueue(int size) {
        this.items = (T[]) new Object[size];
        this.start = 0;
        this.end = -1;
        this.size = 0;
    }


    /**
     * Queue will block until new item can be inserted.
     * @param item the item to insert into the blocking queue
     */
    public synchronized void put(T item) {
        while(this.size >= this.items.length) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        int next = (end + 1) % this.items.length;
        this.items[next] = item;
        this.end = next;
        this.size++;
        if(this.size == 1) {
            this.notifyAll();
        }
    }


    /**
     * Takes an item from the front of the Blocking Queue
     * If the queue is empty, it will wait until an item is inserted
     * @return the item taken off the front of the Blocking Queue
     */
    public synchronized T take() {
        while(size == 0) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        T item = this.items[start];
        this.start = (this.start + 1) % this.items.length;
        this.size--;
        /*
        If the queue was previously full and a new slot has now opened
        notify any waiters in the put method.
         */
        if(this.size == this.items.length-1) {
            this.notifyAll();
        }
        return item;
    }


    /**
     * Takes an item from the front of the Blocking Queue

     * If the queue is empty, it will wait a specified amount of time and check again and return
     * @param millis the amount of milliseconds to wait
     * @return the item if there is one immediately or after the wait, or null if no item after the wait
     */
    public synchronized T poll(int millis) {
        if (this.size == 0) {
            try {
                wait(millis);
                if (this.size == 0) {
                    return null;
                }
            } catch (InterruptedException e) {
                System.out.println("Blocking Queue Poll Interrupted: " + e);
            }

        }
        T item = this.items[start];
        this.start = (this.start + 1) % this.items.length;
        this.size--;
        /*
        If the queue was previously full and a new slot has now opened
        notify any waiters in the put method.
         */
        if(this.size == this.items.length-1) {
            this.notifyAll();
        }
        return item;
    }


    /**
     * Checks if the Blocking Queue is empty
     * @return true if empty, false if not
     */
    public synchronized boolean isEmpty() {
        return this.size == 0;
    }
}