echo -ne '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc localhost 6379
echo -ne '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc localhost 6379
echo -ne '*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n' | nc localhost 6379
echo -ne '*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n' | nc localhost 6379
