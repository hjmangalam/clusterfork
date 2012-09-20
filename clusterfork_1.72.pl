#!/usr/bin/perl -w
# my $| =1; # uncomment to force flushing
# Perldocs removed in favor of the help stanza below at ~ line 634
# search for 'sub usage'

# 1.72 - (08.20.12)small mod to delete empty files before asking to view them.
# 1.71 - (08.17.12) don't even bother including zero-len results (they're empty fer gadsakes).
# 1.70 - (08.07.12) optionally delete zero-len files before viewing in mc for efficiency.
#        the summary still contains the list of zero-len files if you need them. 
# 1.69 - (07.11.12) fixed small bug in specifying multiple hosts in the IPRANGE stanzas and
#        updated initial .clusterforkrc file for better examples.
# 1.68 - (05.03.12) added config to specify which xterm thingy you want to use.
#        set as gnome-terminal to begin with. change it to xterm, terminator, konsole, etc
#        Also, fork the $XTERM so that it leaves the current term able to do something else
# 1.67 - (04.23.12) added '--script' option to do everything silently and NO query
#        to see results.  cf --script ends by returning the results dir
#        but just to return the results dir so that the rest of the script can
#        process the results.
# 1.66 - (01.24.12) corrected subtle PID sed error that led to correct number but
#        empty host list (PIDs sometimes listed with leading space which confused cut
# 1.65 - (01.20.12) added $RESULTS_DIR to write all results.
# 1.64 - (01.01.12) added hostname tracking to ID hung or trailing hosts.
# 1.63 - (12.28.11) fix ';' delimited IPRANGES bug, single hostname problem/process #
# 1.62 - (12.19.11) added --delay to slow the speed of execution of successive commands.
# 1.61 - redirect STDERR into file along with STDOUT.
# 1.60 - fixed a few more oddball characters that bugger up the dir creation.
# 1.59 - fixed system redirection syntax to be more robust(?) across systems.
# 1.58 - added --hosts=[quoted space-delimited hostnames] option to process a few random hosts
# 1.57 - fixed bug in code that searches for alt config file.  Now only writes the 
#        local config file if there is none AND there's no alt config file specified.
# 1.56 - fixed bug introduced in 1.55 that would end the run prematurely - wouldn't get to last
#        number in series; typical off-by-one stupidity.
# 1.55 - fixed bug where if last number in a series was negated, array would overrun, generating
#        an undefined value. (ie a64-[001:040 -012 -013 -040])
# 1.54 - handled single hostname spec in .clusterfork
# 1.52 - added PID tracking and process waiting
# 1.51 -  fixed $padlen for digits that did not have a leading '0'

use strict;
use Getopt::Long;   # for std option handling: -h --yadda=badda, etc
use Config::Simple; # for handling (possibly multiple) configuration files
use Socket;
use Env qw(HOME PATH);

use vars qw( $HOST $CMD $DATEDIR $FORK $DATE $DATEDIR $SHORT_CMD $PWD
$HOSTLIST $IPFILE $CLUSTER $IP_NMBR $md5list  $wclist %md5h %wch $HOME
@md5 @wc $tmp @L $N $DEBUG $line $missing_nodes $QHOST $QFILE %APP $RPMDB
$ALLNODESFILE $cfg %CFG $i $DEBUG $rpmlist $EMAIL_LIST $iter $version $DELAY $DELAY_STR $SCRIPT $LOG
$help $iprange $target $listgroup $configfile  $fork $nofork $cfg %IPRANGE
$sz_lrng @lrng @l $n $u $exp $ttl_IPs $IPGRP $localrange %IPRHA $GRP $PRI_LIST
@pri_names $nbr_pris $ttl $e $f $Ntarget @TARGET @TARGET_IPS @IParr $key $Nels
$Nnames @Names $NOIPR $NONAMES %DONE $Nscrels  @script $altcfg $IPLISTWIDTH
$subrange $NPSbits @PSbits $ver $cur_pid %pidhosts $RESULTS_DIR
$PIDFILE $pidlist @PIDS $active $els $hosts $XTERM
);

$version = <<VERSION;

clusterfork.pl version 1.72
<http://moo.nac.uci.edu/~hjm/clusterfork/index.html>
Copyright Harry Mangalam, OIT, UC Ivine, 2010-2012
<harry.mangalam\@uci.edu>, <hjmangalam\@gmail.com>

This software and documentation is released under
the FSF Affero General Public License (GPL) version 3.
<http://www.gnu.org/licenses/agpl-3.0.html>

VERSION

#$| =1; # uncomment to force flushing

$help = $nofork = 0;
$fork = 1; # default should be to fork
$FORK = "FORK"; # ditto
$iprange = $target = "";
$rpmlist = "";
$target = "";
$NOIPR = 1;
$NONAMES = 1;
$configfile = "";
$altcfg = 0;
$CMD = "";
$PIDFILE = "/.pidfile.cf"; # suffix of pidfile, prefix with $DATDIR below
$hosts="NULL";
$DELAY=0; # the number of s to delay between issuing commands.
$DELAY_STR = "";
$SCRIPT = 0;
$LOG = 0;

&GetOptions(
	"help!"        => \$help,       # dump usage, tips
	"config=s"     => \$configfile, # an alternative config file.
	"target=s"     => \$target,     # name(s) of target groups
	# like --target='ICS_2X,ADC_2X' OR  an IP range as in the config file
	# like 12.23.23.[27:45]
	"listgroup:s"  => \$listgroup, # dumps named groups and their IP #s.
	# (or all of them if given alone)
	"fork!"        => \$fork,      # if defined, fork (default)
	"version!"     => \$ver,
	"hosts=s"      => \$hosts,
	"debug!"       => \$DEBUG,
	"script!"      => \$SCRIPT,
	"delay=s"      => \$DELAY_STR, # delay betw issuing commands (to prevent saturation)
);

if ($ver) {print $version; exit;}
# rationalizing $fork status
if ($fork == 0)  {$FORK = "NULL";$SCRIPT=0;} # if no fork, assume DON'T want $SCRIPT
if ($fork == 1)  {$FORK = "FORK";}

if ($DELAY_STR ne "" ){
	my $dstr = $DELAY_STR;
	$dstr =~ s/,//g; # strip commas
	if ($dstr =~ /\d+(s|S)/){  # only digits or seconds
		chop $dstr; $DELAY = $dstr;
	} elsif ($dstr =~ /\d+(m|M)/) {
		chop $dstr; $DELAY = $dstr * 60;
	} elsif ($dstr =~ /\d+(h|H)/){
		chop $dstr; $DELAY = $dstr * 3600;
	} elsif ($dstr =~ /\d+/ && $dstr !~ /\D/) { $DELAY = $dstr; 
	} else {
		die "\n\nERROR: The '--delay' period can only be s, m, or h.  If you're trying to set up something to wait days between invocations, use cron.\n\ns";
	}
}

if ($DEBUG) {
	print "DEBUG: # of args = $#ARGV \n";
	foreach $i (0..$#ARGV){ print "\targ[$i] = $ARGV[$i]\n"};
}

# check for existence of alt config file if one was spec'ed
if ($configfile ne ""){
	if (-e $configfile && -r $configfile){
	    if ($DEBUG) {print STDERR "DEBUG: Alternative readable clusterforkrc file found at [$configfile]\n";}
	    $cfg = new Config::Simple($configfile);
	    $altcfg = 1;
	} else {die "FATAL: Alt config file [$configfile] doesn't exist or it isn't readable.\n";}
	
# Read the $HOME/.clusterforkrc file to bring in the 'normal' values.
# does it exist
} elsif ($altcfg == 0 && -e "$HOME/.clusterforkrc" && -r "$HOME/.clusterforkrc") {
    if ($DEBUG) {print STDERR "DEBUG: No Alt config file; looking for [$HOME/.clusterforkrc]\n";}
    $cfg = new Config::Simple("$HOME/.clusterforkrc");
} else { # it really is a virgin install.
	print STDERR <<FIRSTTIME;

	It looks like this is the 1st time you've run clusterfork
	as this user on this system.  An example .clusterforkrc file
	will be written to your home dir. Once you edit it to your
	specifications, run a non-destructive command with it
	(ie 'ls -lSh') to make sure it's working and examine the output
	so that you understand the workflow and the output.

	Remember that in order for clusterfork to work, passwordless ssh keys
	must be operational from the node where you execute clusterfork to the
	client nodes.  If you're going to use sudo to execute clusterfork, the
	root user public ssh key must be shared out to the clients.

	Typical cluster use implies a shared /home file system which means that
	the shared keys should only have to be installed once in
	/home/\$USER/.ssh/authorized_keys.

	Please edit the ~/.clusterforkrc template that's just been written so that
	the next time things go smoother.\n

FIRSTTIME

	open CFRC, ">>$HOME/.clusterforkrc" or die "ERROR: Can't write rc file skeleton to $HOME/.clusterforkrc\n";
	print CFRC <<SKELETON;

# This is the config file for the 'clusterfork' application which executes
# commands on a range of machines defined as below.  Use 'clusterfork -h'
# to view the help file
# Comments start with a pound ('#') sign and //cannot share the same line//
# with other configuration data.
# Strings do not need to be quoted unles they contain commas

[ADMIN]
    # RPMDB - file that lists the RPMs that cf has been used to install
    RPMDB = /home/hmangala/BDUC_RPM_LIST.DB

    # emails to notify of newly installed packages
    # Note that you need to escape the '\@' in the list below.
    EMAIL_LIST = "hmangala\@uci.edu, jsaska\@uci.edu, lopez\@uci.edu"

    # how many IP addresses to list on a line - 5 is pretty good.
    IPLISTWIDTH = 5
    
    # write all the results in this dir; coment out or assign to "" if you
    # want to write the results in the current working dir.
    # use fully qualified path; not '~/cf'.
    RESULTS_DIR = "/home/hmangala/cf"

    # command to install apps - if this is found in the command, triggers a routine to
    # email admins with updated install info.
    INSTALLCMD = "yum install -y"
    PATH  = "/usr/local/bin:/sge62/bin/lx24-amd64:/usr/bin:/bin:/usr/sbin"


[SGE]
    # std SGE env variables that you can set here.  In the usual ENV setting,
    # these are prefixed with 'SGE_'.  ie SGE_CELL.
    CELL          = bduc_nacs
    JOB_DIR       = /sge62/bduc_nacs/spool/qmaster/jobs
    EXECD_PORT    = 537
    QMASTER_PORT  = 536
    ROOT          = /sge62


[APPS]
    yum   = /usr/bin/yum
    diff  = /usr/bin/diff
    mutt  = /usr/bin/mutt
    mc    = /usr/bin/mc
    xterm = /usr/bin/gnome-terminal


[IPRANGE]
    # this stanza can be extended arbitrarily to name as many IPRANGEs as you need.
    # these need quotes if they have commas (commas indicate array els)
    ADC_2X = 10.255.78.[10:49]  ;  10.255.78.[77:90]
    ADC_4X = 10.255.78.[50:76]
    ICS_2X = 10.255.89.[5:44]
    CLAWS = 10.255.78.[5:9]
    MIXED = a64-[002:004] ; 10.255.78.[10:12] ; dabrick ; claw5 ; 10.255.78.[14:16]

    # Set temporarily dead nodes in here if required.  Keep the "", if no values to insert.
    # separate hostnames or ranges with ';' as above.

    IGNORE = "10.255.78.12 ; 10.255.78.48 ; 12.23.34.[22:25]"
    # or
    IGNORE = ""
  

    # for GROUPS based on scripts, the value must be in the form of:
    #   [SCRIPT:"whatever the script is"]
    # with required escaping being embedded in the submitted script

    # following QHOST example uses the host-local SGE 'qhost' and 'scut' binaries
    # to generate a list of hosts to process and filters only 'a64' hosts
    # which are responsive (don't have ' - ' entries).
    # this will only work if you use SGE and have installed 'scut' <http://goo.gl/7JiF>

    # QHOST = SCRIPT:"qhost |grep a64 | grep -v ' - ' | scut --c1=0 | perl -e 's/\\n/ /gi' -p"

    # the following is the /ssh remote execution/ of the same script as above.
    # I use it to debug the script and also as an example of how to phrase
    # such remote commands.

    # QHOST = SCRIPT:"ssh hmangala\@bduc-login \"qhost |grep amd64 | grep -v ' - ' |grep -v claw | scut --c1=0 | perl -e 's/\\n/ /gi' -p\""



[GROUPS]

# GROUPS can be composed of primary IPRANGE groups as well as other
    # GROUP groups as long as they have been previously defined.
    ALL_2X = ICS_2X + ADC_2X
    CENTOS = ICS_2X + ADC_2X + ADC_4X
    ADC_ALL = ALL_2X + ADC_4X + CLAWS

SKELETON

   exit(0);
}

# dump usage if it's indicated.
if (($#ARGV < 0 || $help) && (!defined $listgroup)){
   usage();
}

# Now dump the cfg to see how it looks
#if ($DEBUG) {%CFG = $cfg->vars();}
#
%CFG = $cfg->vars(); # load %CFG with all the vars from the config file.
# if (defined $ARGV[0]) {$CMD = $ARGV[0];} # the CMD should be the only thing that's not provided by getopt

$PWD = `pwd`; chomp $PWD;
#$DATE=`date +"%T_%F" | sed 's/:/./g' `; chomp $DATE;
$QHOST = 0;

# from config file.
#$ALLNODESFILE = $CFG{'ADMIN.ALLNODESFILE'}; # not portably implemented yet.
$IPLISTWIDTH = $CFG{'ADMIN.IPLISTWIDTH'};

# these could all just be replaced with the config entry but leave for now.
# SGE Env vars for sudo
$ENV{'SGE_CELL'}         =  $CFG{'SGE.CELL'};
$ENV{'SGE_JOB_DIR'}      =  $CFG{'SGE.JOB_DIR'};
$ENV{'SGE_EXECD_PORT'}   =  $CFG{'SGE.EXECD_PORT'};
$ENV{'SGE_QMASTER_PORT'} =  $CFG{'SGE.QMASTER_PORT'};
$ENV{'SGE_ROOT'}         =  $CFG{'SGE.ROOT'};

# ADMIN def from config file
$RPMDB       = $CFG{'ADMIN.RPMDB'};
$EMAIL_LIST  = $CFG{'ADMIN.EMAIL_LIST'};
$ENV{'PATH'} = $CFG{'ADMIN.PATH'};
$RESULTS_DIR = $CFG{'ADMIN.RESULTS_DIR'};
if (defined $RESULTS_DIR && $RESULTS_DIR ne ""){ # attempt to make the dir
	(-d $RESULTS_DIR) || mkdir $RESULTS_DIR;
	$RESULTS_DIR .= "/"; # to set up the string prefix correctly
} else {$RESULTS_DIR = "";} # results will be written in the current dir.

# test for all the defined APPS
foreach $key (keys %CFG){
	if ($DEBUG) {print STDERR "DEBUG: key: $key =>  $CFG{$key}\n";}
	if ($key =~ /APPS/){
		if ($DEBUG) {print STDERR "\tDEBUG: key: $key =>  $CFG{$key}\n";}
		if (-f $CFG{$key} &&  -x $CFG{$key}) {
			if ($DEBUG) {print "DEBUG: $key: $CFG{$key} is OK\n";}
		} else { die "ERROR: [$CFG{$key}] not found or not executable!! FIX IT!!!\n";}
	}
}
$XTERM = $CFG{'APPS.xterm'};

# the following is specific to
# if a 'yum install', write requested RPMs to DB
if ($CMD =~ /yum install/) {
   my $offset = 12;
   if ($CMD =~ /yum install -y /){$offset = 15;}
   $rpmlist = substr($CMD,$offset);
   open DB, ">>$RPMDB" or die "Can't open [$RPMDB] .. arrrrrrgh!\n";
   print DB "$DATE\t$rpmlist\n";
   close DB;
   if ($DEBUG){&debug(__LINE__, "RPMLIST = [$rpmlist]\n")}
   # & mail to people to tell them a new list is available
   system("cat $RPMDB | $CFG{'APPS.mutt'} -s 'New RPM install list from BDUC'  $EMAIL_LIST");
}


# Create the vars and set up the dir for holding the log info
if (defined $ARGV[0]) {$CMD = $ARGV[0];} # the CMD should be the only thing that's not provided by getopt
$DATE=`date +"%T_%F" | sed 's/:/./g' `; chomp $DATE;
$SHORT_CMD = substr($CMD,0,20); # chop it
# apologies for the following regex..
$SHORT_CMD =~ s/[\n\=\`\"\|\ \\\/\;\~\!\@\#\$\%\^\&\*\+\(\)\{\}\[\]\{\}]/-/g; # and sub '::' for ';'
$DATEDIR = $RESULTS_DIR . "REMOTE_CMD-" . $SHORT_CMD . "-" . $DATE;
$PIDFILE = $DATEDIR . $PIDFILE;
if (defined $FORK && $FORK eq "FORK" && !defined $listgroup){
	#print "INFO: Creating dir [$DATEDIR]..";
	mkdir $DATEDIR or die "ERROR: Can't mkdir [$DATEDIR] at [$PWD]!\n";
} else {$FORK = "NULL";}

if ($SCRIPT) { # if scripting, need to log output for afterwards.
	open (LOG, "> $DATEDIR/LOG");
	select(LOG); # and now all output goes to the LOG until a diff select().
}

# Process targets specified with the commandline option
# If the target is specified on teh commandline, we do the command as we generate the
# IP #s.  If the options specify it by GROUPS or SUPERGROUPS, we have to read in all
# the GROUPS and generate the supergroups and THEN ssh out the commands.


if ($DEBUG) {print STDERR "INFO: Processing targets..\n";}
# like  --target='12.23.24.[45:88]; 34.3.23.[11:45]'
# need to load targets into @TARGET to reference the config groups, and gen IPRANGES
# to load into @TARGET_IPS

# if target has a range implied by [....]
# submit straight to the $NOIPR = 0; # record that we're targeting IP #s
if (($target =~ /\[/ && $target =~ /\]/)|| ($hosts ne "NULL")) { 
	if ($hosts ne "NULL") {$Ntarget = @TARGET = split(/\s+/,$hosts);}
	else {                 $Ntarget = @TARGET = &GenIPArray($target);}
	$IPGRP = "CMDLINE";
	if ($DEBUG) {
		print STDERR "DEBUG: \@TARGET list: @TARGET\n";
	}
		print <<CMDLINE;
======================================================
	Processing nodes specified on commandline
======================================================

CMDLINE


	my $left = 	my $right = 0;
	$NOIPR = 0;
	foreach $HOST (@TARGET) {
		if (!defined $DONE{$HOST}) {
			$IPRHA{$IPGRP}[$left++] = $IParr[$right++];
			#print STDERR "[$HOST] execs [$CMD] [$FORK]\n";
			&host_loop($HOST, $CMD, $DATEDIR, $FORK, $PIDFILE);
			$DONE{$HOST} = 1; # to mark it as done.
		} else {
			print STDERR "WARN: Skipping host [$HOST]. Already processed or part of IGNORE group\n";
			$right++;
		}
	}
}


if ($NOIPR || defined $listgroup) {

	# process the IPRANGES fields to create the IP arrays.
	foreach $key (keys %CFG){
#		print "DEBUG: key: $key, value: $CFG{$key}\n";
		if ($key =~ /IPRANGE/) {
			@l = split /\./, $key;
			$IPGRP = $l[1];
			if ($CFG{$key} =~ /SCRIPT/) { # script has to generate a space-delimited array of hosts
				$Nscrels = @script = split /:/, $CFG{$key};
				if ($Nscrels <3) { # if it's of the form SCRIPT:`script`
					$Nels = @IParr = split /\s+/, `$script[1]`;
	#				if ($DEBUG) {
					if ($DEBUG) {
						print STDERR "\nDEBUG: [$Nels] elements from [$key] script [$script[1]]";
						foreach my $tr (@IParr){print STDERR "$tr ";}
						print STDERR "\n\n";
					}
				}
			} else {
				$IPRANGE{$IPGRP} = $CFG{$key};
				if (($IPRANGE{$IPGRP} =~ '\[' && $IPRANGE{$IPGRP} =~ '\]') || $IPRANGE{$IPGRP} =~ ';') {
				    $Nels = @IParr = &GenIPArray($IPRANGE{$IPGRP});
				} else {  # it's probably a single hostname'
				    $Nels = 1; $IParr[0] = $IPRANGE{$IPGRP}; $#IParr = 0;
				}
			}
			$i = 0;
			foreach $HOST (@IParr){
				if ($IPGRP eq "IGNORE"){
					if ($DEBUG) {print STDERR "DEBUG:\tAdding [$IParr[$i]] to DONE\n";}
					$DONE{$IParr[$i]} = 1; # mark it as 'done'.
				}
				# loading $IPRHA to process later
				$IPRHA{$IPGRP}[$i] = $IParr[$i];
				$i++;
			}
		}
	}

	# and now sum into the 'supergroups'
	foreach $key (keys %CFG){
		if ($key =~ /GROUPS/) {
			@l = split /\./, $key;
			$GRP = $l[1]; # like ADC_ALL
			$PRI_LIST = $CFG{$key}; # like 'ADC_2X + ADC_4X + CLAWS'
			$PRI_LIST =~ s/ //g; # delete all spaces away
			$nbr_pris = @pri_names = split /\+/,$PRI_LIST;
			$ttl = 0;
			for ($e=0; $e<$nbr_pris;$e++) {
				if ($DEBUG) {print STDERR "DEBUG: for Group [$GRP], adding [$pri_names[$e]] payload\n";}
				$f=0;
				while (defined $IPRHA{$pri_names[$e]}[$f]) {
					$IPRHA{$GRP}[$ttl] = $IPRHA{$pri_names[$e]}[$f];
					$ttl++; $f++;
				}
				if ($DEBUG) {print STDERR "DEBUG:\tfor [$pri_names[$e]], array ends at [$f], ttl for [$GRP] now [$ttl]\n";}
			}
		}
	}
	# so now process all the NAMED targets (at this point, it's either a GROUP or SUPERGROUP).
	if ($NOIPR && $target =~ /[\D]*/) { # GROUPS or IPRange groups - same format as GROUPS (NAME1 + NAME2 + Name# ...)
		$NONAMES = 0;
		$Nnames = @Names = split /[ ;,]/, $target;
		foreach my $name (@Names) {
			if (defined $IPRHA{$name}[0]) {
				print <<RESTOFEM;
======================================================
    Processing [$name]
======================================================

RESTOFEM

	#			&pause;
				$i = 0;
				while (defined $IPRHA{$name}[$i]) {
					if (defined $DONE{$IPRHA{$name}[$i]}) {
						print STDERR "WARN: Skipping [$IPRHA{$name}[$i]]. Already processed or part of IGNORE group\n";
					} else {
#					    print "CMD:  host_loop($IPRHA{$name}[$i], $CMD, $DATEDIR, $FORK,$PIDFILE); \n ";
						&host_loop($IPRHA{$name}[$i], $CMD, $DATEDIR, $FORK, $PIDFILE);
						$DONE{$IPRHA{$name}[$i]} = 1; # to mark it as done.
					}
					$i++;
				}
			} else {die "ERROR: UNDEFINED GROUP NAME [$name]\n";}
		}
	} elsif ($NOIPR && $NONAMES) {
		print STDERR "BAD ENDING: NOIPR = [$NOIPR], NONAMES=[$NONAMES]\n";
		&pause;
		die "ERROR: The target you specified [$target] isn't an IP range or a valid GROUP name";
	}
}

# this is a 'good' ending
if ($DEBUG){print STDERR "Good ending: NOIPR = [$NOIPR], NONAMES=[$NONAMES]\n"; &pause;}

# process listgroups
if (defined $listgroup){
		if ($listgroup ne '') {
		$Nnames = @Names = split /[ ;,]/, $listgroup;
		foreach my $name (@Names){
			print "\n\nIP List for [$name]:\n";
			$f = 0;
			while (defined $IPRHA{$name}[$f]){
				my $r = 0;
				while ($r<$IPLISTWIDTH && defined $IPRHA{$name}[$f]) { printf STDERR "$IPRHA{$name}[$f++] "; $r++;}
				print "\\\n"; $r=0;
			}
		}
		print "\n";
	} elsif ($listgroup eq '') { # dump all listgroups
		print "\n\nIP List for ALL GROUPS:\n";
		foreach my $name (keys %IPRHA) {
			print "\nIP List for [$name]:\n";
			$f = 0;
			while (defined $IPRHA{$name}[$f]){
				my $r = 0;
				while ($r<$IPLISTWIDTH && defined $IPRHA{$name}[$f]) { printf STDERR "$IPRHA{$name}[$f++] "; $r++;}
				print "\\\n"; $r=0;
			}
		}
	}
}


if (defined $listgroup) {exit 0;}
local $| = 1;
if ($FORK eq "NULL") {exit(0);} # if we've been doing this serially, there's no need to go further.
# Now organize all the output of the forked processes

open(PID, "<$PIDFILE") or die "Can't open the PID file[$PIDFILE]\n";
my $e =0;
$pidlist = "";
while (<PID>){ # should be only a list of numeric PIDs and hostnames as PID:HOST
	chomp;
	my $n3 = my @l3 = split(/:/,$_);
	$PIDS[$e] = $l3[0];
	$pidhosts{$l3[0]} = $l3[1]; # hash indexed by PID, filter the still-running ones below
	$pidlist .= $PIDS[$e] . " ";
	$e++;
}

$active = $e + 1; # to set up the while loop below
$els = $#PIDS; # get the real size of the starting array of PIDs
print "\n==========================================\n  # of processes at start: [", $els+1, "]\n";


my $secs = 0;
while ($active > 1) { # '1' for 1 line of ps header without any processes
	# keep track of still-running hosts by filtering %pidhosts.
	my $real_procs = 0;
	my $PIDLIST = `ps -p $pidlist`;
	$n = @l = split(/\s+/,`ps -p $pidlist |wc`); # get # of running background processes.
	$active = $l[1]; # should really be '-1' but then would have to '+1'
	$real_procs = $active - 1;
	my $tmpstr = `ps -p  $pidlist  |tail -n+2 | sed "s/^ *//;  s/ \{1,\}/ /g" | cut -f1 -d' '| tr "\n" " " `;
	my $nn = my @ll = split (/\s+/, $tmpstr); # now in a list
	print "\nWaiting for <= [$real_procs] hosts @ [$secs] sec:\n\t";
	my $t = 0;
	foreach my $rpid (@ll){
		$t++;
		print "$pidhosts{$rpid} ";
		if ($t > 10) {print "\n\t"; $t = 0} # insert a newline if it gets too wide.
	}; 
	sleep 2; $secs += 2;
}
print "\n... All slave processes finished! \n\n";

unlink $PIDFILE;  # don't need $PIDFILE anymore

if ($FORK eq "FORK"){
   print "\nYou can find the results of your command in the dir\n\t[ $DATEDIR ]\n";
   $md5list = `cd $DATEDIR; md5sum * | sort`;
   chomp $md5list;
   $wclist = `cd $DATEDIR; wc * | grep -v total | sort -g`;
   chomp $wclist;
   $N = @md5 = split(/\n/,$md5list);
   foreach $line (@md5) {
      chomp $line;
      $line =~ s/^\s+//; # trim off leading whitespace
      $N = @L = split(/\s+/,$line);
      if ($N != 2){print STDERR "ERROR: Unexpected # of fields [$N] splitting md5 input line: [$line])\n";}
      else {
	 $md5h{$L[0]} .= "$L[1]" . " "; # $md5{md5sum} = add to host list
      }
   }

   $N = @wc = split(/\n/,$wclist);  # and now for the wc data

   foreach my $line (@wc) {
      chomp $line;
      $line =~ s/^\s+//; # trim off leading whitespace
      $N = @L = split(/\s+/,$line);
      if ($N != 4){print STDERR "ERROR: Unexpected # of fields [$N] splitting wc input line: [$line]\n";}
      else {$wch{$L[3]} = $L[0] . " " . $L[1]. " " . $L[2];}
   }
   # write analysis to disk in $DATEDIR
   open OUT, ">$DATEDIR/Summary" or die "Can't open the summary file\n";
   print OUT "Summary of contents for files in $DATEDIR\n";
   print OUT "Each line denotes MD5 identical output; wordcount shows similarity\n";
   print OUT "Command: [$CMD]\n";
   if ($QHOST) {print OUT "\n NOTE: MISSING NODES:\n$missing_nodes\n";}
   print OUT "========================================================================\n";

	print OUT " line / word / chars | # |  hosts ->\n";   foreach $key (keys %md5h){
    my $n = my @l = split(/\s+/, $md5h{$key});
	printf OUT "%20s  %3d  %s\n", $wch{$l[0]}, $n, $md5h{$key}; # wc #  host_list
   }
   close OUT;
   
    # delete all the empty files now, so they're not left in the results dir if user doesn't want to see them.
	opendir(DIR, "$DATEDIR");
	my @FILES = readdir(DIR); 
	foreach my $f (@FILES){ if (-z "$DATEDIR/$f") { unlink "$DATEDIR/$f"; } }

   select(STDOUT);
   if ($SCRIPT){ print "$DATEDIR"; exit 0; }
   system("less -S $DATEDIR/Summary");

# already checked for 'mc' in the APP test loop
print "\nWould you like to view the results with 'mc'? [Yn]  ";
	$tmp = <STDIN>;
	if (($tmp !~ /[Nn]/) && (-x $XTERM)) {
		print "\nin a new [x]term or via this [t]ext terminal? [Tx]?";
		$tmp = <STDIN>;
		if ($tmp =~ /[xX]/){ exec("cd $DATEDIR; $XTERM -e mc &"); }
		else {system("cd $DATEDIR; mc;")}
	} elsif ($tmp !~ /[Nn]/) {system("cd $DATEDIR; mc;")}
	else {print "\nHave it your own way.  Bye!\n\n";}
}

##############################################################################
###############################  subroutines  ################################
##############################################################################

sub GenIPArray($) {
# functionize this as GenIPArray(string)
# where string is like ( 1.2.3.[11:25 34:56] 2.3.4.[134:167] 4.2.3.[197:233] )
# and return @ARRAY 1 per el so it can go direct into @TARGET\
# this is probably the better place to pad the #s with leading 0's
# ie: if the input numbers have leading zeros, take the length of the input and
# pad the output in the same way. ie: 0001:0089 or even 00001:89 will pad to 5 chars

	my $ipstr = shift;
	my ($localrange, $single, $DEBUG, $sz_lrng, $exp, @lrng, $e, $ttl_IPs, $n, @l, $u, $nn,
	    @ll, @all, @iparr, @hnarr, $prefix, $suffix, $padlen );
	# split multiple ranges ( 1.2.3.[11:25 34:56] ; 2.3.4.[134:167] ; 4.2.3.[197:233] )
	$n = @l = split /\s*;\s*/, $ipstr; # must split on ';' for the /multiple/ ranges
	# iterate thru each expansion  (1.2.3.[34:56] -> [1.2.3.34][ 1.2.3.35] .. [1.2.3.56]
	
	# now have to do checking on each type of range
	foreach $subrange (@l) { # iter over ranges, can be different types as well (mixed IP, hostnames
		$single = 0;
		# if no [], then it's a single host/hostname, so append it
		if ($subrange !~ /[\[\]]/) {push(@all, $subrange);$single=1;}
		if ($ipstr =~ /[a-zA-Z]+/ && $single == 0) {
			$ttl_IPs = 0; $suffix = $prefix = "";
			$NPSbits = @PSbits = split /[\[\]]/, $subrange; # split on the range indicators
			if ($NPSbits > 3) {die "ERROR: hostname spec has too many parts [$subrange]";}
			my $prefix= $PSbits[0];
			if ($NPSbits == 3) {$suffix = $PSbits[2];} # suffix optional
			$exp = $PSbits[1];
			my $ff = my @nn = split /:/,$exp;
			if ($nn[0] =~ /^0+/){ # if the 1st number has leading zeros
				$padlen = length($nn[0]); # record it for all the ranges
				# and trim off the leading 0s before sending to column_ranges
				# process a # like '00056'
 				while ($nn[0] =~ /^0/) {$nn[0] = substr $nn[0],1;}
			} else { # count the digits - some nodes will use up all the digits like 'a64-188'
				$padlen = length($nn[0]); # record it for all the ranges
			}

			$sz_lrng = @lrng = column_ranges($exp,0);
			# this stanza has to address hostnames ranges like:
			#   a64-[00076:00099 -88 -90].bduc
			foreach $iter (@lrng){  # glue the bits together again
				my $fexp = sprintf "%0*d", $padlen, $iter; # re-pad the number to the full length
				$hnarr[$ttl_IPs] = $prefix . $fexp . $suffix;
				$ttl_IPs++;
			}
			push(@all, @hnarr);
		} elsif ($ipstr =~ /\d+\.\d+\.\d+\./ && $single==0) { # if IP #s,  ranges
			$ttl_IPs = 0; $suffix = $prefix = "";
			for ($u=0; $u<$n; $u++) {
				$nn = @ll = split /\./, $l[$u];
				if ($nn != 4) {die "ERROR: Input IP range <$l[$u]> isn't valid! (count = [$nn]\n";}
				$localrange = $ll[0] . '.' . $ll[1] . '.' . $ll[2] . '.';
				if ($ll[3] =~ /\[/ && $ll[3] =~ /\]/) {
					$exp = substr $ll[3],1,-1; # trim the [ and ]
					$sz_lrng = @lrng = column_ranges($exp,0);
				} else {  # else it's a single number
					$sz_lrng = 1; $lrng[0] = $ll[3];
				}
				# create the full IP #s in the hash
				for ($e=0;$e<$sz_lrng;$e++) {
					$iparr[$ttl_IPs] = $localrange . $lrng[$e];
					$ttl_IPs++;
				}
				if ($DEBUG) {print STDERR "DEBUG:[$IPGRP]: [$e] elements, [$ttl_IPs] total IP #s\n";}
			}
			push(@all, @iparr);
		} else { # what else could it be
		}
	}
	return @all;
}

#
# call as host_loop($HOST, $CMD, $DATEDIR, $$CMD, $PIDFILE)
sub host_loop($$$$$) {

	$HOST = shift;
	$CMD = shift;
	$DATEDIR = shift;
	$FORK = shift;
	my $IP_NMBR = "";
	my $HOSTNAME = "";
	if ($HOST =~ /[a-z]/) { # then it's a hostname, so have to look up the IP#
	    my $packed_ip = gethostbyname($HOST);
	    if (defined $packed_ip) { $IP_NMBR = inet_ntoa($packed_ip); }
	    else {$IP_NMBR = "IP UNDEFINED";}
	    # following is Cox.net's default return for an unresolvable IP #.
	    if ($IP_NMBR =~ "72.215.225.9") {die "\nFATAL: Unresolvable Hostname [$HOST]\n\n";}
	    $HOSTNAME = $HOST;
    } else { # it's already an IP#, so look up the hostname
	    $HOSTNAME = gethostbyaddr(inet_aton($HOST), AF_INET)
	       or die "Can't resolve $HOST: $!\n";
	    $IP_NMBR = $HOST; # and copy for output.
	}

	print "Host: $HOSTNAME [$IP_NMBR]: \n";
	my $PING_RESPONSE = `ping -c1 $HOST`;
	if ( $PING_RESPONSE =~ /100% packet loss/ || $PING_RESPONSE =~ /unknown/) {
		print "                              .... unresponsive or host not found..\n";
	} elsif ($FORK eq "FORK") {
	     if ( -d $DATEDIR ){
			# system-exec the command, with stdout going to the file in the
			# $DATEDIR and the PID going to the $PIDFILE (in $DATEDIR)
			# v 1.61 - added the '&' to redirect STDERR
			# write the $PIDFILE like PID:HOST
			system("ssh $HOST '$CMD' &> $DATEDIR/$HOST & echo \"\${!}\:${HOST}\" >> $PIDFILE ");
			if ($DELAY != 0) {print "\t(delaying \'$DELAY_STR\' before next command.)\n";}
			sleep $DELAY;
	     } else { die "ERROR: Dir $DATEDIR doesn't exist.\n"; }
	 } else {
		system("ssh $HOST $CMD"); # this should dump the command output to STDOUT.
	 }
}

sub pause {
   print "\tWaiting for [Enter]\n";
   $tmp = <STDIN>;
}

# call as [debug(__LINE__, "string")] to print line # and debug string
sub debug($$) {
	my $line = shift;
	my $msg = shift;
	print STDERR "DEBUG[$line]: $msg\n";
	pause;
}


sub usage {
   my $helpfile="$HOME/clustexec_help.tmp";
   my $helptxt = "";
   open HLP, ">$helpfile" or die "Can't open the summary file\n";
   $helptxt = <<HELP;

SUMMARY
=======
clusterfork automates the remote execution of commands to sets of nodes
(typically in a cluster).  Depending on the 'fork' option, it will execute
the commands serially or in parallel.  If the former, the output of each node
will be echoed to STDOUT.  When invoked with the --fork (or no fork option),
it will operate in parallel and pipe the output of the commands into a new,
dated dir, each file labeled with the IP# of the node on which it has
executed, along with a Summary file that contains the original command and
some primitive analysis on the output files, clustering the files by 'wc' &
'md5sum' to show which groups of outputs are identical or nearly so.
The non-empty files can then be viewed via 'Midnight Commander' (mc), 
in a new xterm or the same text terminal.  (The empty results files are 
deleted before you view the results in mc; but are noted in the Summary file.)

COMMAND-LINE OPTIONS in detail
==============================

Usage:  {sudo} clusterfork {options} 'remote command'
   where 'remote command' is the command to be sent to the remote nodes
   and {options} are:

      --help / -h............. dump usage, tips

      --version / -v ......... dump version #

      --config=/alt/config/file .. an alternative config file.
          On 1st execution, clusterfork will write a template config file
          to ~/.clusterforkrc and exit.  You must edit the template to
          provide parameters for your particular setup.

     --target=[quoted IP_Range or predefined GROUP name]
          where IP_Range -> 12.23.23.[27:45 -33 54:88]
            or  'a64-[00023:35 -25:-28].bduc'
          (Note that leading zeros in the FIRST range specifier will be
          replicated in the output; the above pattern will generate:
            a64-00023.bduc, a64-00024.bduc, a64-00029.bduc, etc)

          where GROUP    -> 'ICS_2X,ADC_4X,CLAWS' (from config file)
          (see docs for longer exposition on IP_Ranges and GROUP definition)

      --hosts=[quoted space-delimited hostnames]
          For those times when you have a random number of hostnames to send.
          ie: --hosts='a64-111  a64-142  a64-183  a64-972'.  This will process
          them in the same way as '--target' above, but without having to spec
          the ranges.
          
      --delay=#  (or #s, #m, #h) 
          introduces a this many (s)econds, (m)inutes, or (h)ours between successive 
          commands to nodes to prevent overutilization of resources.  ie between 
          initiating a 'yum -y upgrade' to prevent all the nodes from hitting a 
          local repository simultaneously.  If no s|m|h is given, seconds are assumed.
          Fractions (0.01h) are fine.
      
      --listgroup=[GROUP,GROUP..] (GROUPs from config file)
          If no GROUP specified, dumps IP #s for ALL GROUPS.
          This option does not require a 'remote command' and ignores it
          if given.

      --fork (default) Sends 'remote command' to all nodes in parallel
          and saves output from nodes into a dated subdir for later perusal.
          If you submit a command to run in parallel, it must run to completion
          without intervention.  ie: to install on a CentOS node, the 'yum install'
          command must have the '-y' flag as well: 'yum install -y' to signify
          that 'Y' is assumed to be the answer to all questions.

          If you use the --fork command (as above)), instead of producing the
          stdout/err immediately, a new subdir will be created with the name
          of the format:
             REMOTE_CMD-(20 chars of the command)-time_date
          and the output for each node will be directed into a separate file
          named for the IP number or hostname (whichever the input spec was).
    
      --script runs the command as a script with all the normal screen output 
          sent to the LOG file in the output directory, along with all the normal
          output.  Verify that the command works before you run it with --script.

      --nofork .... Execs 'remote command' serially to each specified node and
          emits the output from each node as it's generated.  If executed with
          this option, it will produce a list of stanzas corresponding to the
          nodes requested:
          --------------------------------
          a64-101 [192.168.0.10]:
            <output from the command>

           a64-102 [192.168.0.11]:
             <output from the command>
          --------------------------------

      --debug ..... causes voluminous debug messages to spew forth

AUTHOR
------
Harry Mangalam <hjm\@tacgi.com>, <harry.mangalam\@uci.edu>
Copyright (c) 2009-2012 UC Irvine

This software and documentation is released under
the FSF Affero General Public License (GPL) version 3.
<http://www.gnu.org/licenses/agpl-3.0.html>

!!!!!!!!!!!!!!!!  Be careful what you ask for. !!!!!!!!!!!!!!!!!!!!

HELP

print HLP $helptxt;
close HLP;

system("less -S $helpfile");
unlink $helpfile;
die "Did that help?.\n"
}

sub trim($) {
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}


sub column_ranges($$) {
    # this sub takes in a column specifier string of the format:
    # '13 2 6 4 8 8 3' (all +#s -> print these cols in this order (duplicates allowed)
    # '3:7 9 11:19 -14:-17 22:23' mixed +, - ranges.  generates an output of:
    # [3 4 5 6 7 9 11 12 13 18 19 22 23] (the - ranges negate the +ranges specified)
    # 'ALL -3 -7:-13' prints all columns in order EXCEPT 3 7 8 9 10 11 12 13
    # note that this routine handles col indices in L->R order and mantains that order.
    # sub column_ranges(@col_str) { ... return @order } # @order is int array that contains order of rationalized cols
    # this sub should be callable to mask the @pos with the @neg and return the result (result could be placed
    # in the @pos to be returned.. This should be callable for any set of inputs.
    # so optimally, the original column selection string is sent in and the equalized string is emitted (or an array of ints
    # that has all the columns in the proper order.


    # $Nc1i = @c1i = column_ranges($ics); #example of call - string goes in, array comes out.

    my $ics = shift;
    my $DEBUG = shift;
    my (@cols_neg, @cols_pos, $cn, $cp, @final, $nbits, @cbits, $nn, @ll);
	$cn = $cp = 0;

    if (($ics=~ /-/) && ($ics !~ /\d:\d/) && ($ics !~ /ALL/i ) && ($ics !~ / \d/)) {
        # then it's negatives only in ranges or singles, so ADD the implied ALL
        $ics = "ALL " . $ics;
        if ($DEBUG) {print  STDERR "added ALL to all-negative run\n"; }
    }
    if (($ics =~ /:/ || $ics =~ /-/) && ($ics !~ /ALL/i )) { # make sure that if the var = 'ALL' it stays 'ALL'

        if ($DEBUG) {print STDERR  "\$ics: range or negative, but NO ALL\n"; }
        # so it could be -c1='-3:-40'
        $ics = trim($ics); # trim both ends of whitespace
        # break it into bits on spaces
        $nbits = @cbits = split(/\s+/,$ics);
        for (my $e=0; $e<$nbits; $e++) {
            #print STDERR "cbits[$e] = $cbits[$e]\n";
            if ($cbits[$e] =~ /\d:[-\d]/) {  # 23:45 or -34:-23  but not '12:' or ':67'
                $nn = @ll = split(/:/,$cbits[$e]); # splits b:e to [b] [e]

                if ($ll[0]<0 && $ll[1]>0 ||$ll[0]>0 && $ll[1]<0 ) {die "A column range crosses 0: [$ll[0] to $ll[1] - This is nonsense!  Try again\n";}

                if ($ll[0] > $ll[1]) { # -20:-22
                    for (my $i=$ll[0]; $i>=$ll[1]; $i--) { # note $i decrements
                        if ($i>=0) {$cols_pos[$cp++] = $i; } #print "+"; # put positive #s in pos array
                        else {
                        	#print STDERR "cols_neg[$cn]=$i\n";
                        	$cols_neg[$cn++] = $i;
                       	} #print "-";      # and negative #s in neg array
                    }
                } else { # b < e (usual case)
#					print STDERR "DEBUG: colranges[810] $ll[0] $ll[1]\n";
#					&pause();
                    for ($i=$ll[0]; $i<=$ll[1]; $i++) { # note $i increments
                        if ($i>=0) {$cols_pos[$cp++] = $i;} # print "+"; # put positive #s in pos array
                        else {$cols_neg[$cn++] = $i; } #print "-";      # and negative #s in neg array
                    }
                }
            } else { # it will be a single number like 2 or 45 or -45
                if ($cbits[$e]>=0) {$cols_pos[$cp++] = $cbits[$e]; } # put positive #s in pos array
                else {  # and negative #s in neg array
                	$cols_neg[$cn++] = $cbits[$e];
                }
            }
        }
        # now all components are in the @cols_etc array, so now need to delete
        # those that have negative  references ie can have a range of
        #   --c1='11:19 -14 -25 24:26  46'
        # and the '-14 would negate the '14' implied by '11:22'.
        # so in above case the pos array would be:
        # [11 12 13 14 15 16 17 18 19 24 25 26 46]
        # and the neg array would be
        # [-14 -25]
        # and the negs should erase the pos's so the ending array in the pos array would be:
        # [11 12 13 -1  15 16 17 18 19 24  -1  26 46] (use  -1 in the cols_pos to indicate a skip
        # if $cols_pos[] < 0, don't print it. if it's +, print it in that order.

        foreach my $neg (@cols_neg) {
	    for (my $pos=0; $pos<=$#cols_pos; $pos++) {
		    if (abs($neg) == $cols_pos[$pos]) {
		    $cols_pos[$pos] = -1; 
		}
            }
        }
        # @ loop end, all the matches are replaced with -1s; now copy them to @tmp, skipping the -1s
        my @tmp;
        my $tc = 0;
		for (my $i=0; $i<=$#cols_pos; $i++) {
		    while ($i <= $#cols_pos && $cols_pos[$i] == -1) {$i++;}
			if ($i <= $#cols_pos) {
			    $tmp[$tc] = $cols_pos[$i];
			    $tc++;
			}
		}
        return @tmp;
    } elsif ($ics =~ /ALL/i) { # ALL makes sense only if you ask for ALL alone or with a
        # set of (-)s (so warn if detect a positive in there as well
        # so break it into bits and extract the (-)s. this will result in an array of negatives
        # that will have to be checked as we print out the cols.
        # means that we'll have to have 2 modes:
        #   print_pos (print ONLY the columns noted) if (defined $col[$i]) {print col_pos[$i
        #   print_neg (print ALL the columns EXCEPT the columns noted)
        #   and then 'ALL' alone signifies to print all columns.
        $ics = trim($ics);
        if ($ics eq "ALL" || $ics eq "all"){ # should test before entry also
#            $final[0] = "ALL"; $final[1] = "STOP";
            $final[0] = "ALL";
            return @final;
        }
        $nbits = @cbits = split(/\s+/,$ics);
        for ($e=0; $e<$nbits; $e++) {
            # one of the bits is ALL cuz that's how we got here. we want to fill in the rest of the (-)s
            if ($DEBUG) {print  STDERR "CBITS = $cbits[$e] \n";}
            if ($DEBUG) {pause(__LINE__);} # $cbits[$e]
            if ( $cbits[$e] =~ /-\d/) { # look for a -#
                if ($cbits[$e] =~ /:/){ # a range
                    $nn = @ll = split(/:/,$cbits[$e]);
                    if ($ll[0]>0 || $ll[1]>0) {
                        die "One of the ranges has a +# in it which doesn't make sense if you specify 'ALL' as well\n";
                    }
                    if ($ll[0] > $ll[1]) {my $tmp = $ll[0]; $ll[0]= $ll[1]; $ll[1]=$tmp;}  # b > e  -4:-6; flip em
                    for ($i=$ll[0]; $i<=$ll[1]; $i++) { # note $i decrements
                        if ($i>0) {die "Don't want a (+) number with ALL; only (-)s\n";} # emit error
                        else {$cols_neg[$cn++] = $i;}      # put negative #s in neg array
                    }
                } else { $cols_neg[$cn++] = $cbits[$e];}  # it's a single so just paste the # in as a neg
            } elsif ($cbits[$e] !~ /ALL/i && int($cbits[$e]) > -1) {
                die "One of #s you specified [$cbits[$e]] is + which doesn't make sense if you specify 'ALL' as well\n";
            }
        }
        # return @cols_neg (all (-)s) and when test for 'ALL' when printing, also test for (-)s in the @arr.
        if ($DEBUG) {print  STDERR "about to return \@col_neg\n"; pause(__LINE__);}
        return @cols_neg;
    } elsif ($ics =~ /\d/ && $ics !~ /-/) { @final = split(/\s+/, $ics); return @final;} #  should be only #s like '2 5 3 7 6'
    else {die "There's something wrong with the column specification [$ics]\n";}
}

