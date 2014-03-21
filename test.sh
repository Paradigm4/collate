iquery -aq "load_library('collate')"
iquery -aq "collate(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5))"

iquery -aq "collate(apply(build(<v:double>[i=1:9,3,0],i),w,i+0.5))"
