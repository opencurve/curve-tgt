Curve is a distributed storage system for QEMU, iSCSI clients.
It provides highly available block level storage volumes that
can be attached to QEMU-based virtual machines. The volumes can
also be attached to other virtual machines and operating
systems run on baremetal hardware if they support iSCSI
protocol. Curve scales to several hundreds nodes, and supports
advanced volume management features such as snapshot, cloning.

With tgt, Curve volume can be used by iSCSI initiators.
Below is a brief description of setup.

1. Install and launch tgt

$ git clone https://github.com/skypexu/tgt.git
$ cd tgt
$ git checkout curvebs
$ make
# make install

2. Setup iSCSI target provided by tgt

One logical unit corresponds to one Curve volume. In this step, we
create iSCSI target and logical unit which can be seen by iSCSI
initiator.

# tgtd
# tgtadm --op new --mode target --tid 1 --lld iscsi --targetname iqn.2022-01.org.opencurve_io
# tgtadm --op new --mode lu --tid 1 --lun 2 --bstype curve --backing-store cbd:pool//iscsi_test_
# tgtadm --lld iscsi --op bind --mode target --tid 1 -I ALL

The parameter --bstype and --backing-store which are required by tgtadm
when we create the logical unit in the target (the third line
of the above commands). With these parameters, we tell the tgtd
process how to connect to the Curve server, which Curve volume we
use as the logical unit.

3. Setup iSCSI session (example of the open-iscsi initiator on Linux)

After setting up iSCSI target, you can use the Curve volume from any virtual
machines and operating systems which supports iSCSI initiator. Many of
popular hypervisors and operating systems support it (e.g. VMware ESX
Family, Linux, Windows, etc). In this example, the way of Linux +
open-iscsi is described.

At first, you have to install open-iscsi ( http://www.open-iscsi.org/
) and launch it. Major linux distros provide their open-iscsi
package. Below is a way of installation in Debian and Ubuntu based
systems.

# apt-get install open-iscsi
# /etc/init.d/open-iscsi start

Next, we need to let iscsid discover and login to the target we've
already created in the above sequence. If the initiator is running
on different host from the target, you have to change the IP
addresses in the below commands.

# iscsiadm -m discovery -t st -p 127.0.0.1
# iscsiadm -m node --portal 127.0.0.1:3260 --login

New device files, e.g. /dev/sdz, will be created on your system after
login completion. you can check the new devices by 'ls /dev/disk/by-path'.
Now your system can use the Curve volume like ordinal HDDs.

