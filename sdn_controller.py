#!/usr/bin/env python3
"""Ryu OpenFlow 1.3 controller with shortest-path forwarding and link-failure rerouting."""
import collections
import json
import threading
import time

from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, DEAD_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import ethernet, ether_types, ipv4, packet
from ryu.ofproto import ofproto_v1_3

APP_NAME = "job_queue_sdn"
ROUTE_COOKIE = 0xD15EA5E
ROUTE_PRIORITY = 100

SWITCHES = {"s1": 1, "s2": 2, "s3": 3, "s4": 4}
SWITCH_NAMES = {value: key for key, value in SWITCHES.items()}

# Keep these addresses and attachment ports in sync with topology.py.
HOSTS = {
    "10.0.0.1": {"name": "c1", "switch": "s1", "port": 1},
    "10.0.0.2": {"name": "c2", "switch": "s1", "port": 2},
    "10.0.0.10": {"name": "broker", "switch": "s2", "port": 1},
    "10.0.0.11": {"name": "w1", "switch": "s3", "port": 1},
    "10.0.0.12": {"name": "w2", "switch": "s3", "port": 2},
}

# Each edge stores the switch-side ports at both ends. Use the same explicit ports in topology.py.
EDGE_PORTS = {
    ("s1", "s2"): {"s1": 3, "s2": 2},
    ("s1", "s4"): {"s1": 4, "s4": 1},
    ("s2", "s3"): {"s2": 4, "s3": 3},
    ("s2", "s4"): {"s2": 3, "s4": 2},
    ("s3", "s4"): {"s3": 4, "s4": 3},
}


class JobQueueSDNController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"wsgi": WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(JobQueueSDNController, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.lock = threading.RLock()
        self.edge_up = {edge: True for edge in EDGE_PORTS}
        self.edge_for_port = {}
        for edge, ports in EDGE_PORTS.items():
            for switch_name, port_no in ports.items():
                self.edge_for_port[(SWITCHES[switch_name], port_no)] = edge
        self.active_routes = {}
        self.port_stats = collections.defaultdict(dict)
        self.last_reroute = {"reason": "controller startup", "time": time.time()}
        kwargs["wsgi"].register(JobQueueSDNREST, {"sdn_app": self})
        self.stats_thread = hub.spawn(self._poll_port_stats)
        self.logger.info("Project 3 SDN controller started; OpenFlow 1.3 / REST port 8080")

    def _edge_key(self, source, destination):
        key = tuple(sorted((source, destination)))
        return key if key in EDGE_PORTS else None

    def _neighbours(self, switch_name):
        result = []
        for edge, ports in EDGE_PORTS.items():
            if not self.edge_up.get(edge, True):
                continue
            if switch_name == edge[0]:
                result.append((edge[1], ports[switch_name], ports[edge[1]]))
            elif switch_name == edge[1]:
                result.append((edge[0], ports[switch_name], ports[edge[0]]))
        return result

    def shortest_path(self, source_switch, destination_switch):
        if source_switch == destination_switch:
            return [source_switch]
        queue = collections.deque([[source_switch]])
        visited = {source_switch}
        while queue:
            path = queue.popleft()
            current = path[-1]
            for neighbour, _local_port, _remote_port in self._neighbours(current):
                if neighbour in visited:
                    continue
                next_path = path + [neighbour]
                if neighbour == destination_switch:
                    return next_path
                visited.add(neighbour)
                queue.append(next_path)
        return None

    def _port_to_neighbour(self, current, neighbour):
        edge = self._edge_key(current, neighbour)
        if edge is None or not self.edge_up.get(edge, True):
            return None
        return EDGE_PORTS[edge][current]

    def _install_flow(self, datapath, source_ip, destination_ip, output_port):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        match = parser.OFPMatch(
            eth_type=ether_types.ETH_TYPE_IP,
            ipv4_src=source_ip,
            ipv4_dst=destination_ip,
        )
        actions = [parser.OFPActionOutput(output_port)]
        instructions = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        flow_mod = parser.OFPFlowMod(
            datapath=datapath,
            cookie=ROUTE_COOKIE,
            priority=ROUTE_PRIORITY,
            match=match,
            instructions=instructions,
        )
        datapath.send_msg(flow_mod)

    def _install_pair(self, source_ip, destination_ip, path):
        destination = HOSTS[destination_ip]
        for index, switch_name in enumerate(path):
            dpid = SWITCHES[switch_name]
            datapath = self.datapaths.get(dpid)
            if datapath is None:
                continue
            if index == len(path) - 1:
                output_port = destination["port"]
            else:
                output_port = self._port_to_neighbour(switch_name, path[index + 1])
            if output_port is not None:
                self._install_flow(datapath, source_ip, destination_ip, output_port)

    def _delete_route_flows(self, datapath):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        delete = parser.OFPFlowMod(
            datapath=datapath,
            cookie=ROUTE_COOKIE,
            cookie_mask=0xFFFFFFFFFFFFFFFF,
            table_id=ofproto.OFPTT_ALL,
            command=ofproto.OFPFC_DELETE,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            match=parser.OFPMatch(),
        )
        datapath.send_msg(delete)

    def reinstall_all_routes(self, reason):
        with self.lock:
            if len(self.datapaths) < len(SWITCHES):
                self.logger.info("Waiting for switches: connected=%s/%s", len(self.datapaths), len(SWITCHES))
                return
            for datapath in list(self.datapaths.values()):
                self._delete_route_flows(datapath)
            routes = {}
            for source_ip, source in HOSTS.items():
                for destination_ip, destination in HOSTS.items():
                    if source_ip == destination_ip:
                        continue
                    path = self.shortest_path(source["switch"], destination["switch"])
                    key = source_ip + "->" + destination_ip
                    if path is None:
                        routes[key] = None
                        continue
                    self._install_pair(source_ip, destination_ip, path)
                    routes[key] = path
            self.active_routes = routes
            self.last_reroute = {"reason": reason, "time": time.time()}
            reachable = sum(1 for path in routes.values() if path is not None)
            self.logger.info("[SDN] Recomputed routes: %s/%s host pairs reachable; reason=%s", reachable, len(routes), reason)

    def _set_link_state(self, source, destination, up, reason):
        edge = self._edge_key(source, destination)
        if edge is None:
            raise ValueError("unknown inter-switch edge; valid switch names are s1, s2, s3, s4")
        with self.lock:
            changed = self.edge_up.get(edge) != up
            self.edge_up[edge] = up
        if changed:
            status = "UP" if up else "DOWN"
            self.logger.warning("[SDN] Link %s-%s marked %s by %s", edge[0], edge[1], status, reason)
            self.reinstall_all_routes(f"link {edge[0]}-{edge[1]} {status.lower()} ({reason})")
        return changed

    def state_snapshot(self):
        with self.lock:
            links = [
                {"source": edge[0], "destination": edge[1], "up": self.edge_up.get(edge, True),
                 "ports": EDGE_PORTS[edge]}
                for edge in sorted(EDGE_PORTS)
            ]
            routes = {
                key: value for key, value in sorted(self.active_routes.items())
            }
            port_stats = {
                SWITCH_NAMES.get(dpid, str(dpid)): {
                    str(port_no): counters for port_no, counters in sorted(entries.items())
                }
                for dpid, entries in self.port_stats.items()
            }
            host_port_map = {
                host_name: {"switch": switch_name, "port": port_no,
                            "counters": port_stats.get(switch_name, {}).get(str(port_no))}
                for host_name, switch_name, port_no in [
                    ("broker", "s2", 1), ("w1", "s3", 1), ("w2", "s3", 2)
                ]
            }
            return {
                "controller": "Project 3 - Distributed Job Queue SDN Controller",
                "openflow": "1.3",
                "connected_switches": sorted(SWITCH_NAMES.get(dpid, str(dpid)) for dpid in self.datapaths),
                "links": links,
                "reachable_host_pair_routes": sum(1 for path in routes.values() if path is not None),
                "total_host_pair_routes": len(routes),
                "routes": routes,
                "last_reroute": self.last_reroute,
                "broker_worker_port_counters": host_port_map,
                "note": "Port counters update periodically. Route paths are recomputed when a link is toggled or reports link-down.",
            }

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        self.datapaths[datapath.id] = datapath
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        instructions = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        table_miss = parser.OFPFlowMod(datapath=datapath, priority=0, match=match, instructions=instructions)
        datapath.send_msg(table_miss)
        switch_name = SWITCH_NAMES.get(datapath.id, str(datapath.id))
        self.logger.info("[SDN] Switch %s connected (DPID=%s)", switch_name, datapath.id)
        if len(self.datapaths) >= len(SWITCHES):
            self.reinstall_all_routes("all four switches connected")

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == DEAD_DISPATCHER:
            with self.lock:
                existed = self.datapaths.pop(datapath.id, None)
            if existed is not None:
                self.logger.warning("[SDN] Switch %s disconnected", SWITCH_NAMES.get(datapath.id, datapath.id))

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        port_no = msg.desc.port_no
        edge = self.edge_for_port.get((datapath.id, port_no))
        if edge is None:
            return
        ofproto = datapath.ofproto
        if msg.reason == ofproto.OFPPR_DELETE:
            up = False
        else:
            up = not bool(msg.desc.state & ofproto.OFPPS_LINK_DOWN)
        self._set_link_state(edge[0], edge[1], up, "OpenFlow port-status event")

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        counters = {}
        for item in ev.msg.body:
            counters[item.port_no] = {
                "rx_packets": item.rx_packets,
                "tx_packets": item.tx_packets,
                "rx_bytes": item.rx_bytes,
                "tx_bytes": item.tx_bytes,
            }
        with self.lock:
            self.port_stats[dpid] = counters

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        pkt = packet.Packet(msg.data)
        ip_header = pkt.get_protocol(ipv4.ipv4)
        if ip_header is None:
            # topology.py installs permanent neighbor entries, so ARP broadcasts are not expected.
            return
        source_ip = ip_header.src
        destination_ip = ip_header.dst
        if source_ip not in HOSTS or destination_ip not in HOSTS:
            return
        source = HOSTS[source_ip]
        destination = HOSTS[destination_ip]
        path = self.shortest_path(source["switch"], destination["switch"])
        if path is None:
            self.logger.warning("[SDN] No route for %s -> %s; packet dropped", source_ip, destination_ip)
            return
        self._install_pair(source_ip, destination_ip, path)
        datapath = msg.datapath
        switch_name = SWITCH_NAMES.get(datapath.id)
        if switch_name not in path:
            return
        index = path.index(switch_name)
        if index == len(path) - 1:
            output_port = destination["port"]
        else:
            output_port = self._port_to_neighbour(switch_name, path[index + 1])
        if output_port is None:
            return
        actions = [datapath.ofproto_parser.OFPActionOutput(output_port)]
        packet_out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=msg.match.get("in_port", datapath.ofproto.OFPP_CONTROLLER),
            actions=actions,
            data=msg.data if msg.buffer_id == datapath.ofproto.OFP_NO_BUFFER else None,
        )
        datapath.send_msg(packet_out)
        self.logger.info("[SDN] First-packet path %s -> %s: %s", source_ip, destination_ip, " -> ".join(path))

    def _poll_port_stats(self):
        while True:
            hub.sleep(5)
            with self.lock:
                datapaths = list(self.datapaths.values())
            for datapath in datapaths:
                try:
                    request = datapath.ofproto_parser.OFPPortStatsRequest(
                        datapath, 0, datapath.ofproto.OFPP_ANY
                    )
                    datapath.send_msg(request)
                except Exception as exc:
                    self.logger.debug("Port-stats request failed for DPID %s: %s", datapath.id, exc)


class JobQueueSDNREST(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(JobQueueSDNREST, self).__init__(req, link, data, **config)
        self.sdn_app = data["sdn_app"]

    @route(APP_NAME, "/sdn/state", methods=["GET"])
    def state(self, req, **kwargs):
        body = json.dumps(self.sdn_app.state_snapshot(), indent=2).encode("utf-8")
        return Response(content_type="application/json", body=body)

    @route(APP_NAME, "/sdn/link/{source}/{destination}/{status}", methods=["POST"])
    def link_control(self, req, **kwargs):
        source = kwargs.get("source", "")
        destination = kwargs.get("destination", "")
        status = kwargs.get("status", "").lower()
        if status not in ("up", "down"):
            return Response(status=400, content_type="application/json",
                            body=json.dumps({"error": "status must be up or down"}).encode("utf-8"))
        try:
            changed = self.sdn_app._set_link_state(source, destination, status == "up", "REST API")
        except ValueError as exc:
            return Response(status=400, content_type="application/json",
                            body=json.dumps({"error": str(exc)}).encode("utf-8"))
        result = {"ok": True, "changed": changed, "source": source, "destination": destination, "status": status}
        return Response(content_type="application/json", body=json.dumps(result).encode("utf-8"))
