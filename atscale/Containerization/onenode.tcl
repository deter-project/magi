set ns [new Simulator] 
source tb_compat.tcl 

set magi_start "sudo python /share/magi/current/magi_bootstrap.py" 

set node [$ns node]
tb-set-node-startcmd $node "$magi_start"

$ns rtproto Static
$ns run
