@load base/init-bare.zeek

@load base/frameworks/logging
@load policy/tuning/json-logs.zeek

@load base/protocols/ftp
@load base/protocols/http
@load base/protocols/conn   

redef LogAscii::use_json = T;

# Parámetros
redef AF_Packet::buffer_size = 1024*1024*1024; # 1 GiB
redef AF_Packet::block_size  = 262144; # 256 KiB
redef AF_Packet::enable_fanout = T;

redef udp_inactivity_timeout = 5sec;
redef icmp_inactivity_timeout = 5sec;
redef tcp_inactivity_timeout = 10sec;

redef tcp_SYN_timeout = 10sec;   
redef tcp_attempt_delay = 3sec;

redef ignore_checksums = T;