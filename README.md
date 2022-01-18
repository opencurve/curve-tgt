网易高性能版本tgt
================

## 1. 修改tgt的目的

本tgt是基于 <A>https://github.com/fujita/tgt</A> 为基础修改而来，目的是为了利用多cpu
的能力。我们观察到原版tgt使用单线程epoll来处理iscsi命令，在10 Gbit/s网络上甚至更快的
网络上，单线程（也即单cpu）处理iscsi命令的速度已经跟不上需要了，一个线程对付多个target
的情况下，多个ISCSI initiator的请求速度稍微高一点，这个单线程的cpu使用率就100%忙碌。

## 2.修改策略

### 2.1 使用多个线程做epoll

实现多个event loop线程，每个线程负责一定数量的socket connection上的iscsi命令处理。
这样就能发挥多cpu的处理能力。

### 2.2 为每个target创建一个epoll线程

为了避免多个target共享一个epoll时依然可能出现超过单个cpu处理能力的问题，我们为每一个
target设置了一个epoll线程。 target epoll的cpu使用由OS负责调度，这样在各target上可以
实现公平的cpu使用。当然如果网络速度再快，依然会出现单个epoll线程处理不过来一个iscsi
target上的请求，但是目前这个方案依然是我们能做的最好方案。

### 2.3 管理平面

管理平面保持了与原始tgt的兼容性。从命令行使用方面来说，没有任何区别，没有任何修改。管理
面在程序的主线程上提供服务，主线程也是一个epoll loop线程，这与原始的tgt没有区别，它负责
target, lun, login/logout, discover，session, connection等的管理。ISCSI IO线程
也即每一个服务于一个target上的epoll线程，不修改由管理面管理的target,lun等数据
结构。但是可能会结束会话和connection。当Intiator连接到ISCSI服务器时，总是先被管理平面
线程所服务，如果该connection最后需要创建session去访问某个target，那么该connection会
被迁移到对应的target的epoll线程上去。

### 2.4 数据结构的锁

为每一个target提供一个mutex，当target epoll线程在运行时，这把锁是被epoll线程锁住的，
这样epoll线程可以任意结束一个sesssion或connection，当线程进入epoll_wait时，这把锁是释
放了的，epoll_wait返回时又会锁住这把锁。而管理面要存取、删除一个session或者connection时，
也需要锁住这把锁，这样就可以安全地访问对应target上的session和connection了。

## 3. tgt与curve

我们为tgt提供了访问curve的驱动，详见doc/README.curve，
这样用户就可以在任何支持iscsi的操作系统上使用curve块设备存储，例如Windows。

## 4. 关于iser

iser target服务目前依然归属于主线程服务，因为我们还不具备测试RDMA的条件，所以这部分代码
还没有修改。
