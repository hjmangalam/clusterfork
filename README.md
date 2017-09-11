clusterfork: a cluster admin tool
=================================
by Harry Mangalam <harry.mangalam@uci.edu>
v1.86, Sept 10, 2017

'clusterfork' (aka 'cf') is a commandline perl script for issuing the same
command to many computers simultaneously via ssh, collating the results of
that command by node, and presenting those results to the user in a number
of ways to judge whether it has been successful.  It can be scripted as well
as used interactively, logging the usual output.

For example, the following line will show you the correctable and
uncorrectable memory errors on the nodes 10.255.78.20 thru 10.255.78.60. 
Change the IP range to your own and try it yourself.  Obviously, the
machines have to be using the http://buttersideup.com/edacwiki/Main_Page [EDAC system].

clusterfork --target=10.255.78.[20:60] 'cd /sys/devices/system/edac/mc \
&&  grep [0-9]* mc*/csrow*/[cu]e_count'

Read more at the http://moo.nac.uci.edu/~hjm/clusterfork/clusterfork.html[original] 
(and still operational):


 
