collate
=======

## Synopsis
Map muti-attribute arrays into matrices.

## Rationale
Complicated input data are often loaded into multi-attribute arrays.  Sometimes
we want to treat uniformly-typed subsets of those attribute as a matrix, for
example to compute correlations or regressions with.

## Load the plugin
```
load_library('collate')
help('collate')
{i} help
{0} 'Operator: collate
Usage: collate(A)
where:
A is a 1-d matrix with one or more uniformly-typed attributes.

collate(A) returns a 2-d array that copies the attributes of A into
columns of an output matrix.

Note: The output matrix row dimension will have a chunk size equal
to the input array, and column chunk size equal to the number of columns.
```

## Example
Consider the  1-d input array stored below into `A`:
```
iquery -aq "store(apply(build(<v:double>[i=0:9,3,0],i),w,i+0.5), A)"
{i} v,w
{0} 0,0.5
{1} 1,1.5
{2} 2,2.5
{3} 3,3.5
{4} 4,4.5
{5} 5,5.5
{6} 6,6.5
{7} 7,7.5
{8} 8,8.5
{9} 9,9.5
```

Use `collate` to transform this into a 2-d matrix whose columns correpond to the input array attributes:
```
iquery -aq "collate(A)"
{i,j} val
{0,0} 0
{0,1} 0.5
{1,0} 1
{1,1} 1.5
{2,0} 2
{2,1} 2.5
{3,0} 3
{3,1} 3.5
{4,0} 4
{4,1} 4.5
{5,0} 5
{5,1} 5.5
{6,0} 6
{6,1} 6.5
{7,0} 7
{7,1} 7.5
{8,0} 8
{8,1} 8.5
{9,0} 9
{9,1} 9.5
```

## Installing the plug in

You'll need SciDB installed, along with the SciDB development header packages.
The names vary depending on your operating system type, but they are the
package that have "-dev" in the name. You *don't* need the SciDB source code to
compile and install this.

Run `make` and copy  the `libcollate.so` plugin to the `lib/scidb/plugins`
directory on each of your SciDB cluster nodes. Here is an example:

```
git clone https://github.com/Paradigm4/collate.git
cd collate
make
cp libcollate.so /opt/scidb/13.12/lib/scidb/plugins

iquery -aq "load_library('collate')"
```
Remember to copy the plugin to all you SciDB cluster nodes.
