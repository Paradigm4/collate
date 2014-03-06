iquery -aq "collate(apply(build(<v:double>[i=1:10,3,0],i),w,2*i),a)" && cat /dev/shm/000/0/scidb-stderr.log
