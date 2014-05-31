#Progress.md


Applex:
---

### 关于OmniEngine：

里面实际分为asyncengine（async_consistent_engine)和sync_engine(synchronous_engine)，对于async的，里面有个Scheduler(ischeduler)类。Scheduler是个队列，会一直拿出来做，直到空。

sync类里有个Main Program，就是如何做gather apply scatter的过程
但是这些在start调用之后都会直接做，做完就结束了。

我想到的有两点：

* 首先，是不是应该算结束之后把线程block住或者怎么样，让它继续等待输入
* 其次，如果计算还没有结束就来新数据，是不是可以直接往scheduler里面加东西来让它继续run？

### 关于Csr_Storage:

dynamic_csr_storage已经实现了insert方法。所以我觉得里面应该维护一个变量：需要计算（或者正在计算）的节点数组。在insert的时候和算收敛之后都需要维护这个数组。