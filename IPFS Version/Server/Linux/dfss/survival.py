from scapy.all import sniff
import time

def packet_callback(packet):
    if packet.haslayer("IP") and packet.haslayer("TCP"):
        ip_src = packet["IP"].src
        ip_dst = packet["IP"].dst
        sport = packet["TCP"].sport
        dport = packet["TCP"].dport

        # 只关注特定端口的流量
        target_port = 5656  # 更改为你想要监控的端口

        if dport == target_port or sport == target_port:
            with open("listen.csv", "a+") as f:
            	f.write("%s, %s, %s, %s, %s, %s\n" % (time.time(), ip_src, sport, ip_dst, dport, len(packet)))
            #print(f"Captured packet from {ip_src}:{sport} to {ip_dst}:{dport}")
            #print("size: %s" % len(packet))

if __name__ == "__main__":
    # 监听指定网卡上的所有流量
    # 可以通过在参数中指定`iface`来选择特定网卡
    sniff(prn=packet_callback, store=0)
