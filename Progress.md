Progress.md

* 关于OmniEngine：
里面实际分为asyncengine（async_consistent_engine)和sync_engine(synchronous_engine)，对于async的，里面有个Scheduler(ischeduler)类。Scheduler是个队列，会一直拿出来做，直到空。
sync类里有个Main Program，就是如何做gather apply scatter的过程
但是这些在start调用之后都会直接做，做完就结束了。
我想到的有两点：
