
> ~/scidbdata/000/0/scidb-stderr.log
> ~/scidbdata/000/1/scidb-stderr.log

iquery -naq "remove(a)" 2>/dev/null
iquery -aq "create_array(a, <x:double>[i=0:9,3,0,j=0:1,2,0])"
iquery -aq "load_library('collate')"
iquery -aq "collate(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5),a)"

cat ~/scidbdata/000/0/scidb-stderr.log
cat ~/scidbdata/000/1/scidb-stderr.log
echo
