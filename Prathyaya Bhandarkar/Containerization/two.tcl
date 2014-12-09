set ns [new Simulator]
source tb_compat.tcl

set a [$ns node]
set b [$ns node]

set link0 [ $ns duplex-link $a $b 100Mb 0ms DropTail]

$ns rtproto Static
$ns run 

