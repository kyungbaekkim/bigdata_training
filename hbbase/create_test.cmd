create 'test', 'cf'
list 'test'
put 'test','key1','cf:a','value1'
put 'test','key2','cf:a','value2'
put 'test','key3','cf:b','value3'
put 'test','key4','cf:c','value4'
scan 'test'
get 'test','key1'
exit
