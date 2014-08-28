iquery -aq "load_library('collate')"
iquery -aq "build(<v:double>[i=1:9,3,0],i)"
echo
iquery -aq "collate(build(<v:double>[i=1:9,3,0],i))"
echo
iquery -aq "collate(apply(build(<v:double>[i=1:9,3,0],i),w,i+0.5))"
