#!/bin/bash

#$ -q all.q
#$ -pe mpi_pe 2
#
# ...specify the SGE "all.q" queue and **also** the "mpi_pe" PE...
# to be used with qsub on SGE
#

#$ -cwd
#$ -S /bin/bash

export PATH=$PATH:/opt/openmpi-1.6.5/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/openmpi-1.6.5/lib
#
# ...ensure the OpenMPI-related executables and libaries can be found by
#    the job...
#

mpirun -np $NSLOTS hostname
#
# ...start the job!  "$NSLOTS" is the number of cores/processes/slots the
#    job will use --- the value is set by the orte.pe PE.
#