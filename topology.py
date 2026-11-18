#!/usr/bin/env python3
"""Four-switch Mininet topology with primary and alternate paths for Project 3."""
from mininet.cli import CLI
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.topo import Topo

SWITCH_DPIDS = {
    "s1": "0000000000000001",
    "s2": "0000000000000002",
    "s3": "0000000000000003",
    "s4": "0000000000000004",
}

HOSTS = [
    ("c1", "10.0.0.1/24", "00:00:00:00:00:01", "s1", 1),
    ("c2", "10.0.0.2/24", "00:00:00:00:00:02", "s1", 2),
    ("broker", "10.0.0.10/24", "00:00:00:00:00:10", "s2", 1),
    ("w1", "10.0.0.11/24", "00:00:00:00:00:11", "s3", 1),
    ("w2", "10.0.0.12/24", "00:00:00:00:00:12", "s3", 2),
]


class JobQueueTopo(Topo):
    def build(self):
        switches = {}
        for name, dpid in SWITCH_DPIDS.items():
            switches[name] = self.addSwitch(
                name, dpid=dpid, protocols="OpenFlow13", failMode="secure"
            )
        for host_name, ip, mac, switch_name, switch_port in HOSTS:
            host = self.addHost(host_name, ip=ip, mac=mac)
            self.addLink(host, switches[switch_name], port2=switch_port)

        # Port numbers must remain aligned with EDGE_PORTS in sdn_controller.py.
        self.addLink(switches["s1"], switches["s2"], port1=3, port2=2)  # primary client-broker path
        self.addLink(switches["s1"], switches["s4"], port1=4, port2=1)  # alternate path, first hop
        self.addLink(switches["s2"], switches["s4"], port1=3, port2=2)  # alternate core link
        self.addLink(switches["s2"], switches["s3"], port1=4, port2=3)  # primary broker-worker path
        self.addLink(switches["s3"], switches["s4"], port1=4, port2=3)  # alternate worker path


def install_static_neighbours(net):
    hosts = list(net.hosts)
    for source in hosts:
        interface = str(source.defaultIntf())
        for destination in hosts:
            if source.name == destination.name:
                continue
            command = (
                f"ip neigh replace {destination.IP()} lladdr {destination.MAC()} "
                f"nud permanent dev {interface}"
            )
            source.cmd(command)
    info("*** Installed permanent neighbor entries (the controller can use unicast IPv4 flows).\n")


def main():
    setLogLevel("info")
    net = Mininet(
        topo=JobQueueTopo(),
        controller=None,
        switch=OVSSwitch,
        autoSetMacs=False,
        autoStaticArp=False,
        build=True,
    )
    net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)
    try:
        info("*** Starting Mininet topology. Start sdn_controller.py before running this file.\n")
        net.start()
        connected = net.waitConnected(timeout=10)
        if not connected:
            info("*** WARNING: not all switches confirmed controller connectivity within 10 seconds. Check Ryu logs/port 6653.\n")
        install_static_neighbours(net)
        info("*** Topology ready: clients=c1,c2; broker=broker; workers=w1,w2; switches=s1-s4.\n")
        info("*** Test connectivity with: pingall\n")
        info("*** Primary-path failure demo: link s1 s2 down  (then use the REST API command in README).\n")
        info("*** Worker failure demo: terminate the w1 worker process from another terminal.\n")
        CLI(net)
    finally:
        info("*** Stopping Mininet and cleaning up links.\n")
        net.stop()


if __name__ == "__main__":
    main()
